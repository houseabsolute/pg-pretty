package R::RustWriter;

use v5.30;
use strict;
use warnings;
use autodie qw( :all );
use experimental qw( signatures );
use feature qw( postderef );

use String::CamelSnakeKebab qw( upper_camel_case );

use Moose::Role;

## no critic (TestingAndDebugging::ProhibitNoWarnings)
no warnings qw( experimental::postderef experimental::signatures );
## use critic

sub _type ( $, $comment, $what, $name, $attributes, @contents ) {
    my $code = q{};
    $code .= _clean_comment($comment);
    $code .= "$_\n"
        for map { _attribute( $_->@* ) }
        sort _sort_attributes $attributes->@*;
    $code .= "pub $what $name {\n";

    for my $line (@contents) {
        $code .= _clean_comment( $line->{comment}, 4 );
        if ( $line->{attributes} ) {
            $code .= "$_\n"
                for map { _attribute( $_->@*, 4 ) }
                sort _sort_attributes $line->{attributes}->@*;
        }
        $code .= "    $line->{line},";
        $code .= " // $line->{inline_comment}"
            if $line->{inline_comment};
        $code .= "\n";
    }
    $code .= "}\n";

    return $code;
}

sub _attribute ( $name, $params, $indent = 0 ) {
    my $attr = "#[$name";
    if ($params) {
        $attr .= _attribute_params($params) ;
    }
    $attr .= "]";

    return $attr;
}

sub _attribute_params ($params) {
    my $attr = '(';
    if ( ref $params eq 'HASH' ) {
        $attr .= join ', ',
            map {"$_ = $params->{$_}"} sort keys $params->%*;
    }
    elsif ( ref $params eq 'ARRAY' ) {
        $attr .= join ', ', $params->@*;
    }
    else {
        $attr .= "$params";
    }
    $attr .= ')';

    return $attr;
}

# everything else is 0
my %attr_order = (
    derive => 1,
    serde  => 2,
);

sub _sort_attributes {
    my @a = $a->@*;
    my @b = $b->@*;

    if ( $a[0] ne $b[0] ) {
        my $a_order = $attr_order{ $a[0] } // 0;
        my $b_order = $attr_order{ $b[0] } // 0;
        return $a_order <=> $b_order || $a cmp $b;
    }

    shift @a;
    shift @b;
    my $a_params = @a ? _attribute_params(@a) : q{};
    my $b_params = @b ? _attribute_params(@b) : q{};

    return $a_params cmp $b_params;
}

sub _c_to_rust_name ( $, $name ) {
    # We can't just have 'pub struct String' because that breaks all use of
    # the core String type.
    my $ucc = $name eq 'String' ? 'StringStruct' : upper_camel_case($name);
    return $ucc =~ s/Aexpr/AExpr/r;
}

sub _clean_comment ( $comment, $indent = 0 ) {
    return q{} unless defined $comment && $comment =~ /\S/;

    my @lines = grep {length}
        grep { !/\A[- ]+\z/ }
        map {s<\A\s*[/ ]*\s*\*\s*><>r} map {s<\s*\*/\z><>r}
        split /\n/, $comment;

    my $prefix = q{ } x $indent;
    return join q{}, map {"$prefix// $_\n"} @lines;
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

sub _is_reserved_word ( $, $word ) {
    return $reserved{$word};
}

1;
