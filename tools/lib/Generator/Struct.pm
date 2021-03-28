package Generator::Struct;

use v5.30;
use strict;
use warnings;
use autodie qw( :all );
use experimental qw( signatures );
use feature qw( postderef );

use Specio::Library::Builtins;
use Specio::Library::String;
use String::CamelSnakeKebab qw( lower_snake_case upper_camel_case );

use Moose;

with 'R::RustWriter';

## no critic (TestingAndDebugging::ProhibitNoWarnings)
no warnings qw( experimental::postderef experimental::signatures );
## use critic

has c_name => (
    is       => 'ro',
    isa      => t('NonEmptyStr'),
    required => 1,
);

has comment => (
    is  => 'ro',
    isa => t( 'Maybe', of => t('Str') ),
);

has fields => (
    is       => 'ro',
    isa      => t( 'ArrayRef', of => t('HashRef') ),
    required => 1,
);

has known_enums => (
    is       => 'ro',
    isa      => t('HashRef'),
    required => 1,
);

has known_structs => (
    is       => 'ro',
    isa      => t('HashRef'),
    required => 1,
);

has _rust_name => (
    is      => 'ro',
    isa     => t('NonEmptyStr'),
    lazy    => 1,
    default => sub ($self) { $self->_c_to_rust_name( $self->c_name ) },
);

has _referenced_structs => (
    is      => 'ro',
    isa     => t('HashRef'),
    default => sub { {} },
);

has _referenced_enums => (
    is      => 'ro',
    isa     => t('HashRef'),
    default => sub { {} },
);

sub referenced_structs ($self) {
    return keys $self->_referenced_structs->%*;
}

sub referenced_enums ($self) {
    return keys $self->_referenced_enums->%*;
}

sub as_rust ($self) {
    my @attr = (
        [ derive                => [qw( Debug Deserialize PartialEq )] ],
        [ skip_serializing_none => undef ],
    );

    my $c_name    = $self->c_name;
    my $rust_name = $self->_rust_name;
    push @attr, [ serde => { rename => qq{"$c_name"} } ]
        if $c_name ne $rust_name;

    return $self->_type(
        $self->comment,
        'struct',
        $rust_name,
        \@attr,
        map { $self->_one_field($_) } $self->fields->@*,
    );
}

sub _one_field ( $self, $field ) {
    return unless $field->{name};
    return if $field->{name} eq 'type';
    return if $field->{name} eq 'xpr';

    # These two are function pointers.
    return if $field->{c_type} =~ /Hook$/;
    return if $field->{c_type} eq 'FdwRoutine*';

    my %field = (
        comment        => $field->{comment},
        inline_comment => $field->{c_type},
    );

    my $c_name    = $field->{name};
    my $rust_name = lower_snake_case($c_name);
    my $rust_type = $self->_rust_type($field);

    my @attr;
    if ( $rust_type eq 'bool' ) {
        push @attr, [ serde => 'default' ];
    }

    # This means the C name is something like "fooField", not "foo_field".
    if ( $c_name ne $rust_name ) {
        push @attr, [ serde => { rename => qq{"$c_name"} } ];
    }
    $field{attributes} = \@attr
        if @attr;

    my $r_quote = $self->_is_reserved_word($rust_name) ? 'r#' : q{};
    $field{line} = "pub $r_quote$rust_name: $rust_type";

    return \%field;
}

# This maps rust struct names to their _C_ field names, and the rust type for
# that field. This is mostly for cases where the C field type is something
# like Node* or List*, but we know from examining the gram.y source that the
# possible types are much narrower than "all nodes".
my %overrides = (
    AIndirection => { indirection => 'Vec<IndirectionListElement>' },
    AExpr        => { rexpr       => 'OneOrManyNodes' },
    Alias        => { colnames    => 'Option<Vec<StringStruct>>' },
    ColumnRef    => { fields      => 'Vec<ColumnRefField>' },
    Constraint   => { exclusions  => 'Option<Vec<Exclusion>>' },
    CreateStmt   => { tableElts   => 'Option<Vec<CreateStmtElement>>' },
    DefElem      => { arg         => 'ValueOrTypeName' },
    Float        => { str         => 'String' },
    InsertStmt   => {
        selectStmt => 'Option<SelectStmtWrapper>',
        cols       => 'Option<Vec<ResTargetWrapper>>',
    },
    LockingClause  => { lockedRels => 'Option<Vec<RangeVarWrapper>>' },
    RangeFunction  => { functions  => 'Vec<RangeFunctionElement>' },
    RangeSubselect => { subquery   => 'Box<SelectStmtWrapper>' },
    RawStmt        => {
        stmt => 'Node',
    },
    ResTarget  => { indirection => 'Option<Vec<IndirectionListElement>>' },
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
    AConst => 'val',
    AExpr  => [
        qw(
            lexpr
            rexpr
        )
    ],
    AIndices                   => 'uidx',
    AIndirection               => 'arg',
    Alias                      => 'aliasname',
    AlterDatabaseStmt          => 'options',
    AlterDefaultPrivilegesStmt => 'options',
    AlterDomainStmt            => 'typeName',
    AlterEnumStmt              => 'typeName',
    AlterExtensionStmt         => 'options',
    AlterFunctionStmt          => 'options',
    AlterOperatorStmt          => 'options',
    AlterOpFamilyStmt          => 'opfamilyname',
    AlterRoleStmt              => 'options',
    AlterSubScriptionStmt      => 'options',
    BitString                  => 'str',
    BoolExpr                   => [
        qw(
            boolop
            args
        )
    ],
    ColumnDef         => 'colname',
    CompositeTypeStmt => 'coldeflist',
    Constraint        => 'contype',
    CopyStmt          => 'options',
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
    CreateExtensionStmt => 'options',
    CreateFunctionStmt  => [
        qw(
            funcname
            options
            returnType
        )
    ],
    CreateStmt    => 'relation',
    CurrentOfExpr => 'cursor_name',
    DefElem       => [
        qw(
            defname
            arg
        )
    ],
    DeleteStmt => [
        qw(
            relation
        )
    ],
    FuncCall          => 'funcname',
    FunctionParameter => 'argType',
    IndexElem         => 'nulls_ordering',
    IndexStmt         => [
        qw(
            relation
            indexParams
        )
    ],
    InsertStmt => 'relation',
    Integer    => 'ival',
    JoinExpr   => [
        qw(
            jointype
            larg
            rarg
        )
    ],
    LockingClause  => 'strength',
    MultiAssignRef => [
        qw(
            source
            colno
            ncolumns
        )
    ],
    OnConflictClause   => 'action',
    PartitionBoundSpec => 'strategy',
    PartitionSpec      => [
        qw(
            strategy
            partParams
        )
    ],
    RangeSubselect   => 'subquery',
    RangeTableSample => [
        qw(
            relation
            method
            args
        )
    ],
    RangeVar => 'relname',
    RowExpr  => [
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
    SQLValueFunction => 'op',
    SubLink          => [
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
    UpdateStmt => [
        qw(
            relation
            targetList
        )
    ],
    Value => 'val',
);
$not_optional{$_}
    = { map { $_ => 1 }
        ref $not_optional{$_} ? $not_optional{$_}->@* : $not_optional{$_} }
    for keys %not_optional;

# This makes nearly everything an Option<...>, which is almost surely
# wrong. But it's not possible to figure out what's supposed to be optional
# from the JSON files (or the C source, really). If fields aren't marked
# optional than serde blows up on deserializing because of missing fields. We
# can override individual fields to de-Option them in the %overrides hash,
# which is gross and tedious to maintain, but at least doable.
sub _rust_type ( $self, $field ) {
    my $field_name = $field->{name};
    if ( $overrides{ $self->_rust_name } ) {
        return $overrides{ $self->_rust_name }{$field_name}
            if exists $overrides{ $self->_rust_name }{$field_name};
    }

    my $base_type = $self->_rust_base_type($field);

    # The various *kind fields all describe what the struct is, so I assume
    # they're required.
    return $base_type if $field_name =~ /kind\z/;
    return $base_type if $not_optional{ $self->_rust_name }{$field_name};
    return $base_type if $base_type eq 'bool';

    return "Option<$base_type>";
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

sub _rust_base_type ( $self, $field ) {
    my $c_type = $field->{c_type};

    my $comment = $field->{comment} // q{};
    return 'Vec<Vec<StringStructWrapper>>'
        if $comment =~ /[Ll]ist of list of (?:Value strings|String)/;
    return 'Vec<StringStructWrapper>'
        if $comment
        =~ /[Ll]ist of (?:Value strings|String|\(T_String\) Values)/;

    if ( $comment =~ /[Ll]ist of ([A-Z][A-Za-z]+)(?: nodes)?/ ) {
        my $name = $1;
        if ( $self->known_enums->{$name} ) {
            $self->_referenced_enums->{$name} = 1;
            return 'Vec<' . upper_camel_case($name) . '>';
        }
        elsif ( $self->known_structs->{$name} ) {
            $self->_referenced_structs->{$name} = 1;
            return 'Vec<' . upper_camel_case($name) . 'Wrapper>';
        }
    }

    if ( $self->known_enums->{$c_type} ) {
        $self->_referenced_enums->{$c_type} = 1;
        return upper_camel_case($c_type);
    }

    if ( $self->known_structs->{$c_type} ) {
        $self->_referenced_structs->{$c_type} = 1;
        return upper_camel_case($c_type) . 'Wrapper';
    }

    my $no_ptr_type = $c_type =~ s/\*\z//r;

    if ( $self->known_enums->{$no_ptr_type} ) {
        $self->_referenced_enums->{$no_ptr_type} = 1;
        return upper_camel_case($no_ptr_type);
    }

    if ( $self->known_structs->{$no_ptr_type} ) {
        $self->_referenced_structs->{$no_ptr_type} = 1;
        return upper_camel_case($no_ptr_type) . 'Wrapper';
    }

    return $type_map{$c_type} // die "Not sure how to map $c_type to Rust";
}

__PACKAGE__->meta->make_immutable;

1;
