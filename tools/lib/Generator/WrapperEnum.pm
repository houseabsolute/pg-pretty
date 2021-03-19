package Generator::WrapperEnum;

use v5.30;
use strict;
use warnings;
use autodie qw( :all );
use experimental qw( signatures );
use feature qw( postderef );

use Specio::Library::String;

use Moose;

with 'R::RustWriter';

## no critic (TestingAndDebugging::ProhibitNoWarnings)
no warnings qw( experimental::postderef experimental::signatures );
## use critic

has rust_name => (
    is       => 'ro',
    isa      => t('NonEmptyStr'),
    required => 1,
);

sub as_rust ($self) {
    my $rust_name = $self->rust_name;

    my @attr = ( [ derive => [qw( Debug Deserialize PartialEq )] ] );

    my %line = ( line => "$rust_name($rust_name)" );
    if ( $rust_name eq 'StringStruct' ) {
        $line{attributes} = [ [ serde => { rename => qq{"String"} } ] ];
    }

    return $self->_type(
        q{},
        'enum',
        $rust_name . 'Wrapper',
        \@attr,
        \%line,
    );
}

__PACKAGE__->meta->make_immutable;

1;
