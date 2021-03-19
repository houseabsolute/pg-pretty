package Generator::Enum;

use v5.30;
use strict;
use warnings;
use autodie qw( :all );
use experimental qw( signatures );
use feature qw( postderef );

use Specio::Library::Builtins;
use Specio::Library::String;

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

has values => (
    is       => 'ro',
    isa      => t( 'ArrayRef', of => t('HashRef') ),
    required => 1,
);

sub as_rust ($self) {
    my @variants;

    my $int_type = 'u8';

    # It would be nice to sort these, but the JSON parser simply encodes most
    # enums as a number matching that particular variant's position in the
    # enum definition.
    for my $val ( grep { $_->{name} } $self->{values}->@* ) {
        # Enum names are generally ALL CAPS. If we don't lower case them first
        # then we end up with names like AEXPROP instead of "AExprOp".
        my $rust_member = $self->_c_to_rust_name( lc $val->{name} );
        my $rust_val    = $self->_rust_enum_value($rust_member);
        $int_type = 'i8' if $rust_val && $rust_val < 0;

        my %variant     = ( comment => $val->{comment} );
        $variant{line} = defined $rust_val ? "$rust_member = $rust_val" : $rust_member;

        push @variants, \%variant;
    }

    my %derive;

    return $self->_type(
        $self->comment,
        'enum',
        $self->_c_to_rust_name( $self->c_name ),
        [
            [ derive => [qw( Debug Deserialize_repr Display PartialEq )] ],
            [ repr   => $int_type ],
        ],
        @variants,
    );
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

__PACKAGE__->meta->make_immutable;

1;
