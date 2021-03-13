#!/usr/bin/env perl

package E;

use v5.30;
use strict;
use warnings;
use autodie qw( :all );
use experimental qw( signatures );
use feature qw( postderef );

use HTML::Entities qw( decode_entities );
use Path::Tiny qw( path );
use Path::Tiny::Rule;
use SGML::Parser::OpenSP;

use Moose;

## no critic (TestingAndDebugging::ProhibitNoWarnings)
no warnings qw( experimental::postderef experimental::signatures );
## use critic

with 'MooseX::Getopt::Dashes';

has source => (
    is            => 'ro',
    isa           => 'Str',
    required      => 1,
    documentation => 'The directory containing the Postgres source',
);

has file => (
    is            => 'ro',
    isa           => 'Str',
    documentation => 'A single file name to parse',
);

sub run ($self) {
    my $parser = SGML::Parser::OpenSP->new;

    my $cases_dir = path(qw( formatter tests cases potential ));
    $cases_dir->mkpath( 0, 0755 );

    my $file = $self->file;
    my $iter
        = Path::Tiny::Rule->new->name( $file ? qr/\Q$file\E$/ : qr/\.sgml$/ )
        ->iter( path( $self->source )->child(qw( doc src sgml )) );

    my $x = 0;
    while ( my $file = $iter->() ) {
        say "parsing $file";

        my $handler = Handler->new;
        $parser->handler($handler);

        my $sgml = decode_entities( $file->slurp_utf8 );
        $parser->parse_string($sgml);

        my @filtered = _filtered_sql( $handler->sql );
        next unless @filtered;

        my $i = 0;
        my $content = join "\n", map { _case( $_, $i++ ) } @filtered;

        my $cases_file = $cases_dir->child( $file->basename =~ s/\.sgml$//r );
        $cases_file->spew_utf8($content);
    }

    return 0;
}

sub _filtered_sql {
    my $sql = shift;

    return map {
        map      {s/^\n+|\n+$//gr}
            grep {/^CREATE|DROP|ALTER|SET|SELECT|INSERT|UPDATE|DELETE/}
            split /;/, $_
    } $sql->@*;
}

sub _case {
    my $stmt = shift;
    my $i    = shift;

    return <<"EOF";
++++
$i
----
$stmt
----
???
----
EOF
}

__PACKAGE__->meta->make_immutable;

package Handler;

use Moose;
use MooseX::SemiAffordanceAccessor;

has sql => (
    is      => 'ro',
    isa     => 'ArrayRef[Str]',
    default => sub { [] },
);

has _current_sql => (
    is  => 'rw',
    isa => 'Str',
);

has _in_code => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0,
);

sub start_element {
    my $self = shift;
    my $elt  = shift;

    if ( $elt->{Name} eq 'PROGRAMLISTING' ) {
        $self->_set_in_code(1);
        $self->_set_current_sql(q{});
    }
}

sub data {
    my $self = shift;
    my $elt  = shift;

    if ( $self->_in_code ) {
        $self->_set_current_sql( $self->_current_sql . $elt->{Data} );
    }
}

sub end_element {
    my $self = shift;
    my $elt  = shift;

    if ( $elt->{Name} eq 'PROGRAMLISTING' ) {
        $self->_set_in_code(0);
        my $sql = $self->_current_sql;
        if ( $sql ne q{} ) {
            $sql =~ s/\r\n?/\n/g;
            push $self->sql->@*, $sql;
        }
    }
}

package main;

E->new_with_options->run;
