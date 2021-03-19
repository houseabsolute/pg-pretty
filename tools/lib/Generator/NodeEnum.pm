package Generator::NodeEnum;

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

has nodes => (
    is       => 'ro',
    isa      => t( 'ArrayRef', of => t('NonEmptyStr') ),
    required => 1,
);

sub as_rust ($self) {
    my $comment = 'A Node is a type that can be referenced from many different types of parsed statements.';

    my @attr = (
        [ derive => [qw( Debug Display Deserialize PartialEq )] ],
    );

    # RawStmt will never contain itself, so we don't need the enum indirection
    # for it.
    my @variants = map { $self->_one_variant($_) }
        sort grep { $_ ne 'RawStmt' } $self->nodes->@*;

    return $self->_type(
        $comment,
        'enum',
        'Node',
        \@attr,
        @variants,
    );
}

sub _one_variant ( $self, $c_name ) {
    my $rust_name = $self->_c_to_rust_name($c_name);
    my %variant   = ( line => "$rust_name($rust_name)" );
    if ( $c_name ne $rust_name ) {
        $variant{attributes} = [ [ serde => { rename => qq{"$c_name"} } ] ];
    }

    return \%variant;
}

__PACKAGE__->meta->make_immutable;

1;
