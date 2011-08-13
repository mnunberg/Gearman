#!/usr/bin/env perl
use strict;
use warnings;
use Gearman::Healthtest;
use Gearman::Util;
#$Gearman::Util::Timeout = Gearman::Util::NO_TIMEOUT;

my $host = $ARGV[0];
my $tester = Gearman::Healthtest->new($host, 4, "foo");
my $ret = $tester->test(100);
die "Healthtest returned an undefined value" if !defined $ret;
my ($njobs, $duration) = @$ret;
print "Completed $njobs in $duration seconds\n";
print "Trying multi test\n";
my $completed = Gearman::Healthtest::test_multi($host, 5, "foo", batchsize => 10);
print "completed $completed jobs\n";
