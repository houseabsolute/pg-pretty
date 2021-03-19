#!/usr/bin/env perl
use strict;
use warnings;

use FindBin qw( $Bin );
use lib "$Bin/lib";

use Generator;

Generator->new_with_options->run;
