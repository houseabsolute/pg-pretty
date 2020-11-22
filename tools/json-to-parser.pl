#!/usr/bin/env perl

package G;

use v5.30;
use strict;
use warnings;
use autodie qw( :all );
use experimental qw( signatures );
use feature qw( postderef );

use IPC::Run3 qw( run3 );
use JSON::MaybeXS qw( decode_json );
use Path::Tiny qw( path );
use String::CamelSnakeKebab qw( lower_snake_case upper_camel_case );

use Moose;

## no critic (TestingAndDebugging::ProhibitNoWarnings)
no warnings qw( experimental::postderef experimental::signatures );
## use critic

with 'MooseX::Getopt::Dashes';

has from => (
    is            => 'ro',
    isa           => 'Str',
    required      => 1,
    documentation => 'The root directory of the libpg_query code',
);

has _enums => (
    is      => 'ro',
    isa     => 'HashRef',
    lazy    => 1,
    builder => '_build_enums',
);

has _seen_enums => (
    is      => 'ro',
    isa     => 'HashRef',
    lazy    => 1,
    default => sub { {} },
);

has _structs => (
    is      => 'ro',
    isa     => 'HashRef',
    lazy    => 1,
    builder => '_build_structs',
);

has _structs_to_wrap => (
    is      => 'ro',
    isa     => 'HashRef',
    lazy    => 1,
    default => sub {
        {
            SelectStmt   => 1,
            StringStruct => 1,
        }
    },
);

has _node_types => (
    is      => 'ro',
    isa     => 'ArrayRef',
    lazy    => 1,
    builder => '_build_node_types',
);

has to => (
    is            => 'ro',
    isa           => 'Str',
    required      => 1,
    documentation => 'The file to which to write the generated code',
);

sub _build_enums ($self) {
    my $enums
        = decode_json(
        path( $self->from, qw( srcdata enum_defs.json ) )->slurp_utf8 );
    my %enums = map { $_->%* } values $enums->%*;

    # See https://github.com/lfittl/libpg_query/issues/74.
    push $enums{BoolExprType}{values}->@*, { name => 'NOT_EXPR' };
    return \%enums;
}

my %wanted_struct_paths = map { $_ => 1 } qw(
    nodes/parsenodes
    nodes/primnodes
    nodes/value
);

sub _build_structs ($self) {
    my $structs
        = decode_json(
        path( $self->from, qw( srcdata struct_defs.json ) )->slurp_utf8 );
    return {
        map  { $_->%* }
        map  { $structs->{$_} }
        grep { $wanted_struct_paths{$_} } keys $structs->%*
    };
}

sub _build_node_types ($self) {
    return [
        # see https://github.com/lfittl/libpg_query/issues/69
        grep { !/\A[a-z]/ } decode_json(
            path( $self->from, qw( srcdata nodetypes.json ) )->slurp_utf8
        )->@*
    ];
}

my $GeneratedMsg = sprintf( <<'EOF', path($0)->basename );
// This code was generated by the %s script in the pg-pretty repo. Don't edit
// it by hand except for debugging purposes.
EOF

sub run ($self) {
    my $to = path( $self->to );
    die "No file at $to" unless $to->exists;

    my $structs = $self->_structs;

    my $code = q{};
    for my $name ( sort keys $structs->%* ) {
        $code .= $self->_one_struct( $name, $self->_structs->{$name} );
    }

    for my $name ( sort keys $self->_seen_enums->%* ) {
        my $enum = $self->_enums->{$name}
            // die "Could not find an enum named $name";
        $code .= $self->_one_enum( $name, $enum );
    }

    $code .= $self->_node_enums;
    $code .= $self->_struct_wrapper_enums;

    $code =~ s/\n+$//;
    my $orig_content = $to->slurp_utf8;
    my $new_content  = $orig_content
        =~ s{(\n// <<< begin generated code)\n+.+\n(// end generated code >>>)}{$1\n//\n$GeneratedMsg\n$code\n\n$2}sr;

    my ( $stdout, $stderr );
    run3(
        [qw( rustfmt --emit stdout )],
        \$new_content,
        \$stdout,
        \$stderr,
    );
    die "rustfmt stderr:\n$stderr\n$new_content" if $stderr;
    die 'rustfmt exited non-0'                   if $?;

    # Every time we touch this file cargo rebuilds it.
    if ( $stdout ne $orig_content ) {
        $to->spew_utf8($new_content);
        system( 'rustfmt', $to );
    }

    return 0;
}

sub _one_struct ( $self, $name, $struct ) {

    # This is mapped by hand in the ast.rs code.
    return q{} if $name eq 'List';

    my @fields
        = grep { $_->{name} && $_->{name} !~ /\A(?:type|xpr)\z/ }
        $struct->{fields}->@*;

    my $code = q{};

    $code .= _clean_comment( $struct->{comment} )
        if ( $struct->{comment} // q{} ) =~ /\S/;
    $code .= "#[skip_serializing_none]\n";
    $code .= "#[derive(Debug, Deserialize, PartialEq)]\n";

    # We can't just have 'pub struct String' because that breaks all use of
    # the core String type.
    my $rust_name = $self->_rust_name($name);
    if ( $rust_name ne $name ) {
        $code .= qq{#[serde(rename = "$name")]\n};
    }
    $code .= "pub struct $rust_name {\n";
    $code .= $self->_one_field( $rust_name, $_ ) for @fields;
    $code .= "}\n\n";

    return $code;
}

sub _one_field ( $self, $struct_name, $field ) {
    return q{} unless $field->{name};
    return q{} if $field->{name} eq 'type';

    # These two are function pointers.
    return q{} if $field->{c_type} =~ /Hook$/;
    return q{} if $field->{c_type} eq 'FdwRoutine*';

    my $code = q{};
    $code .= _clean_comment( $field->{comment}, q{    } )
        if ( $field->{comment} // q{} ) =~ /\S/;

    my $snake = lower_snake_case( $field->{name} );

    my ( $rust_type, @attr )
        = $self->_rust_field_definition( $struct_name, $field );

    if (@attr) {
        my $attr = join ', ',
            map { $_->@* == 2 ? qq{$_->[0] = "$_->[1]"} : $_->[0] }
            sort { $a->[0] cmp $b->[0] } @attr;
        $code .= "    #[serde($attr)]\n";
    }

    $snake = qq{r#$snake} if is_reserved_word($snake);
    $code .= "    pub $snake: $rust_type, // $field->{c_type}\n";

    return $code;
}

# from https://doc.rust-lang.org/reference/keywords.html
my %reserved = map { $_ => 1 } qw(
    Self
    abstract
    as
    async
    await
    become
    box
    break
    const
    continue
    crate
    do
    dyn
    else
    enum
    extern
    false
    final
    fn
    for
    if
    impl
    in
    let
    loop
    macro
    match
    mod
    move
    mut
    override
    priv
    pub
    ref
    return
    self
    static
    struct
    super
    trait
    true
    try
    type
    typeof
    unsafe
    unsized
    use
    virtual
    where
    while
    yield
);

sub is_reserved_word ($word) {
    return $reserved{$word};
}

sub _one_enum ( $self, $name, $enum ) {
    my $code = q{};
    $code .= _clean_comment( $enum->{comment} )
        if ( $enum->{comment} // q{} ) =~ /\S/;

    my $rust_enum = $self->_rust_name($name);
    $code .= "#[derive(Debug, Deserialize_repr, Display, PartialEq)]\n";
    my $int_type = $self->_rust_enum_int_type($rust_enum);
    $code .= "#[repr($int_type)]\n";
    $code .= "pub enum $rust_enum {\n";

    for my $member ( $enum->{values}->@* ) {
        next unless $member->{name};
        $code .= _clean_comment( $member->{comment}, q{    } )
            if ( $member->{comment} // q{} ) =~ /\S/;
        my $rust_member = $self->_rust_name( lc $member->{name} );

        my $val = $self->_rust_enum_value($rust_member);
        if ( defined $val ) {
            $code .= "     $rust_member = $val,\n";
        }
        else {
            $code .= "    $rust_member,\n";
        }
    }
    $code .= "}\n\n";

    return $code;
}

sub _node_enums ($self) {
    my @nodes = map { [ $_, $self->_rust_name($_) ] }
        sort
        grep { $self->_structs->{$_} } $self->_node_types->@*;

    my $code
        = "// A Node is a type that can be referenced from many different types of parsed statements.\n";
    $code .= "#[derive(Debug, Display, Deserialize, PartialEq)]\n";
    $code .= "pub enum Node {\n";

    # RawStmt will never contain itself, so we don't need the enum indirection
    # for it.
    for my $node ( grep { $_->[1] ne 'RawStmt' } @nodes ) {
        my ( $orig_name, $rust_name ) = $node->@*;
        if ( $orig_name ne $rust_name ) {
            $code .= qq{#[serde(rename = "$orig_name")]\n};
        }
        $code .= "$rust_name($rust_name),\n";
    }
    $code .= "\n}\n\n";

    return $code;
}

# These wrappers are needed because the JSON returned by the Pg internals is
# _always_ tagged. For example, the ColumnDef's typeName field is typed as a
# "TypeName". But the JSON contains something like `"typeName": {"TypeName":
# {...}}`. That extra "TypeName" key will only be handled properly if there is
# a wrapper enum _around_ the struct.
sub _struct_wrapper_enums ($self) {
    my $code = q{};
    for my $name ( sort keys $self->_structs_to_wrap->%* ) {
        my $val = $self->_structs_to_wrap->{$name};
        my $rename
            = $name eq 'StringStruct' ? 'String'
            : ref $val                ? $val->{rename}
            :                           q{};
        my $serde = $rename ? qq{    #[serde(rename = "$rename")]\n} : q{};
        $code .= <<"EOF";
#[derive(Debug, Deserialize, PartialEq)]
pub enum ${name}Wrapper {
$serde    $name($name),
}

EOF
    }

    return $code;
}

sub _clean_comment ( $comment, $indent = q{} ) {
    my @lines = grep {length}
        grep { !/\A[- ]+\z/ }
        map {s<\A\s*[/ ]*\s*\*\s*><>r} map {s<\s*\*/\z><>r}
        split /\n/, $comment;
    return join q{}, map {"$indent// $_\n"} @lines;
}

sub _rust_field_definition ( $self, $struct_name, $field ) {
    my $rust_type = $self->_rust_type( $struct_name, $field );

    my $name = $field->{name};
    my @attr;
    if ( $name ne lower_snake_case($name) ) {
        push @attr, [ rename => $name ];
    }
    if ( $rust_type eq 'bool' ) {
        push @attr, ['default'];
    }

    return $rust_type, @attr;
}

# A lot of this comes from looking at pg_query_go/scripts/generate_nodes.rb
# and the go code for that project.
my %type_map = (
    '[]Node'     => 'Vec<Node>',
    AclMode      => 'AclMode',
    AttrNumber   => 'AttrNumber',
    Bitmapset    => 'Vec<u32>',
    'Bitmapset*' => 'Vec<u32>',
    bits32       => 'u32',
    bool         => 'bool',
    char         => 'char',
    'char*'      => 'String',
    Cost         => 'f64',
    Datum        => 'u64',
    double       => 'f64',
    float        => 'f32',
    Index        => 'Index',
    int          => 'i64',
    int16        => 'i16',
    int32        => 'i32',
    'int32*'     => 'Vec<i32>',
    'List*'      => 'List',
    long         => 'i64',
    NameData     => 'String',
    '[]Node'     => 'Vec<Node>',
    Oid          => 'Oid',
    Relids       => 'Vec<u32>',
    'Relids*'    => 'Vec<u32>',
    Size         => 'u64',
    String       => 'StringStruct',
    uint         => 'u64',
    uint16       => 'u16',
    uint32       => 'u32',
    uintptr_t    => 'u64',
    'Value'      => 'Value',
    'Value*'     => 'Value',
    'void*'      => 'core::ffi::c_void',
);

$type_map{$_} = 'Box<Node>' for 'Node*', 'Expr', 'Expr*';

my %overrides = (
    AExpr          => { rexpr      => 'OneOrManyNodes' },
    Alias          => { colnames   => 'Option<Vec<StringStruct>>' },
    ColumnRef      => { fields     => 'Vec<ColumnRefField>' },
    DefElem        => { arg        => 'DefElemArgs' },
    Float          => { str        => 'String' },
    Integer        => { ival       => 'i64' },
    LockingClause  => { lockedRels => 'Option<Vec<RangeVarWrapper>>' },
    RangeFunction  => { functions  => 'Vec<RangeFunctionElement>' },
    RangeSubselect => { subquery   => 'Box<SelectStmtWrapper>' },
    RawStmt        => {
        stmt => 'Node',
    },
    SelectStmt => {
        distinctClause => 'Option<Vec<Option<Node>>>',
        valuesLists    => 'Option<Vec<List>>',
        op             => 'SetOperation',
        larg           => 'Option<Box<SelectStmtWrapper>>',
        rarg           => 'Option<Box<SelectStmtWrapper>>',
    },
    StringStruct => { str          => 'String' },
    WindowClause => { frameOptions => 'FrameOptions' },
    WindowDef    => { frameOptions => 'FrameOptions' },
);

my %not_optional = (
    AConst => [qw( val )],
    AExpr  => [
        qw(
            lexpr
            rexpr
            )
    ],
    Alias                      => [qw( aliasname )],
    AlterDatabaseStmt          => [qw( options )],
    AlterDefaultPrivilegesStmt => [qw( options )],
    AlterDomainStmt            => [qw( typeName )],
    AlterEnumStmt              => [qw( typeName )],
    AlterExtensionStmt         => [qw( options )],
    AlterFunctionStmt          => [qw( options )],
    AlterOperatorStmt          => [qw( options )],
    AlterOpFamilyStmt          => [qw( opfamilyname )],
    AlterRoleStmt              => [qw( options )],
    AlterSubScriptionStmt      => [qw( options )],
    BitString                  => [qw( str )],
    BoolExpr                   => [
        qw(
            boolop
            args
            )
    ],
    ColumnDef => [
        qw(
            colname
            typeName
            )
    ],
    CompositeTypeStmt => [qw( coldeflist )],
    CopyStmt          => [qw( options )],
    CreateDomainStmt  => [
        qw(
            domainname
            constraints
            )
    ],
    CreateEnumStmt => [
        qw(
            typeName
            vals
            )
    ],
    CreateExtensionStmt => [qw( options )],
    CreateFunctionStmt  => [
        qw(
            funcname
            options
            returnType
            )
    ],
    FuncCall          => [qw( funcname )],
    FunctionParameter => [qw( argType )],
    JoinExpr          => [
        qw(
            jointype
            larg
            rarg
            )
    ],
    LockingClause    => [qw( strength )],
    RangeSubselect   => [qw( subquery )],
    RangeTableSample => [
        qw(
            relation
            method
            args
            )
    ],
    RangeVar  => [qw( relname )],
    ResTarget => [qw( val )],
    RowExpr   => [
        qw(
            args
            row_format
            )
    ],
    SortBy => [
        qw(
            node
            sortby_dir
            sortby_nulls
            )
    ],
    SubLink => [
        qw(
            subLinkType
            subselect
            )
    ],
    TypeCast => [
        qw(
            arg
            typeName
            )
    ],
    TypeName => [qw( names )],
    Value    => [qw( val )],
);
$not_optional{$_} = { map { $_ => 1 } $not_optional{$_}->@* }
    for keys %not_optional;

# This makes nearly everything an Option<...>, which is almost surely
# wrong. But it's not possible to figure out what's supposed to be optional
# from the JSON files (or the C source, really). If fields aren't marked
# optional than serde blows up on deserializing because of missing fields. We
# can override individual fields to de-Option them in the %overrides hash,
# which is gross and tedious to maintain, but at least doable.
sub _rust_type ( $self, $struct_name, $field ) {
    my $field_name = $field->{name};
    if ( $overrides{$struct_name} ) {
        return $overrides{$struct_name}{$field_name}
            if exists $overrides{$struct_name}{$field_name};
    }

    my $base_type = $self->_rust_base_type($field);

    # The various *kind fields all describe what the struct is, so I assume
    # they're required.
    return $base_type if $field_name =~ /kind\z/;
    return $base_type if $not_optional{$struct_name}{$field_name};
    return $base_type if $base_type eq 'bool';

    return "Option<$base_type>";
}

sub _rust_base_type ( $self, $field ) {
    my $c_type      = $field->{c_type};
    my $no_ptr_type = $c_type =~ s/\*\z//r;

    my $comment = $field->{comment} // q{};
    return 'Vec<Vec<StringStructWrapper>>'
        if $comment =~ /[Ll]ist of list of (?:Value strings|String)/;
    return 'Vec<StringStructWrapper>'
        if $comment
        =~ /[Ll]ist of (?:Value strings|String|\(T_String\) Values)/;

    if ( $comment =~ /[Ll]ist of ([A-Z][A-Za-z]+)(?: nodes)?/ ) {
        my $name = $1;
        if ( $self->_enums->{$name} ) {
            $self->_seen_enums->{$name} = 1;
            return 'Vec<' . upper_camel_case($name) . '>';
        }
        elsif ( $self->_structs->{$name} ) {
            $self->_structs_to_wrap->{$name} = 1;
            return 'Vec<' . upper_camel_case($name) . 'Wrapper>';
        }
    }

    if ( $self->_enums->{$c_type} ) {
        $self->_seen_enums->{$c_type} = 1;
        return upper_camel_case($c_type);
    }

    if ( $self->_enums->{$no_ptr_type} ) {
        $self->_seen_enums->{$no_ptr_type} = 1;
        return upper_camel_case($no_ptr_type);
    }

    if ( $self->_structs->{$c_type} ) {
        $self->_structs_to_wrap->{$c_type} = 1;
        return upper_camel_case($c_type) . 'Wrapper';
    }

    if ( $self->_structs->{$no_ptr_type} ) {
        $self->_structs_to_wrap->{$no_ptr_type} = 1;
        return upper_camel_case($no_ptr_type) . 'Wrapper';
    }

    return $type_map{$c_type} // die "Not sure how to map $c_type to Rust";
}

my %enum_int_type_overrides = (
    PartitionRangeDatumKind => 'i8',
);

sub _rust_enum_int_type ( $, $name ) {
    return $enum_int_type_overrides{$name} // 'u8';
}

# I wish this was in the enum_defs.json file -
# https://github.com/lfittl/libpg_query/issues/71.
my %enum_value_overrides = (
    FuncParamIn                 => ord('i'),
    FuncParamOut                => ord('o'),
    FuncParamInout              => ord('b'),
    FuncParamVariadic           => ord('v'),
    FuncParamTable              => ord('t'),
    PartitionRangeDatumMinvalue => -1,
    PartitionRangeDatumValue    => 0,
    PartitionRangeDatumMaxvalue => 1,
);

sub _rust_enum_value ( $, $name ) {
    return $enum_value_overrides{$name};
}

sub _rust_name ( $, $name ) {
    my $ucc = $name eq 'String' ? 'StringStruct' : upper_camel_case($name);
    return $ucc =~ s/Aexpr/AExpr/r;
}

__PACKAGE__->meta->make_immutable;

package main;

G->new_with_options->run;
