#!/usr/bin/perl

#tunables:
use constant GEARMAN_JOB_NAME => "garbage";
use constant INCREMENTS => 100;
use constant UPDATE_INTERVAL => 1;

use Getopt::Long;
use strict;
use warnings;
use Storable;
use List::Util "sum";
use POE qw(Wheel::Run Filter::Reference Filter::Line);
use Time::HiRes "sleep";
use Gearman::Client;
use Gearman::Worker;

use Data::Dumper;

use Fcntl;

#stupid help generator... (argp emulation.. sorta)
my @_gopt;
my $help_text = "$0 <options>\n";

my $Options = {};
Getopt::Long::Configure("no_ignore_case");
my @optlist = (
    ["jobserver", "j", "s@", "list of jobservers to use", undef],
    ["workers", "w", "i", "number of worker processes", 2],
    ["clients", "c", "i", "number of client processes", 2],
    ["bytes", "b", "i", "number of /dev/urandom bytes to send from worker", 8192],
	["background", "B", "i", "clients should background jobs", 0]
);
foreach my $opt_s (@optlist) {
    my ($longopt,$shortopt,$type,$description, $default) = @$opt_s;
    my $optstring = "$longopt|$shortopt=$type";
    push(@_gopt, ($optstring));
    $help_text .= "\t-$shortopt --$longopt\t$description";
    if($default) {
        $Options->{$longopt} = $default;
        $help_text .= " (default=$default)";
    }
    $help_text .= "\n";
}
#defaults
GetOptions($Options, @_gopt, "help|h" => sub { print $help_text; exit(0); });
if(!$Options->{jobserver}) { die "Need jobserver!"; }

my $SUBMIT_FN = ($Options->{background}) ? "dispatch_background" : "do_task";

my $begin_time = time()-1;

sub gearman_client {
    $0 .= " (CLIENT)";
    my $total = 0;
    my $filter = POE::Filter::Reference->new();
    $|++;
    #connect to gearman and make a client
    my $client = Gearman::Client->new();
    $client->job_servers(@{$Options->{jobserver}});
    while(1) {
		my $fn = $client->can($SUBMIT_FN);
		$fn->($client, GEARMAN_JOB_NAME, "foo");
        $total++;
        if($total % INCREMENTS) {
            print @{$filter->put([\INCREMENTS])};
        }
    }
}
sub gearman_worker {
    $0 .= " (WORKER)";
    my $total = 0;
    my $filter = POE::Filter::Reference->new();
    my $worker = Gearman::Worker->new();
    $worker->job_servers(@{$Options->{jobserver}});
    $worker->register_function(
        GEARMAN_JOB_NAME, sub {
            sysopen(my $devrandom, "/dev/urandom", O_RDONLY);
            sysread($devrandom, my $buf, $Options->{bytes});
            close($devrandom);
            return $buf;
        }
    );
    while(1) {
        $total++;
        $worker->work();
        if($total % INCREMENTS) {
            print @{$filter->put([\INCREMENTS])};
        }
    }
}

my %client_totals;
my %worker_totals;

sub mk_update_cb($) {
    my ($hashref) = @_;
    my $handler = sub  {
        $|++;
        my $heap = $_[HEAP];
        $hashref->{$heap->{cmd}} += ${$_[ARG0]};
    };
}

sub stderr_handler {
    $|++;
    print $_[ARG0] . "\n";
}

sub launch_children {
    my ($child_fn, $update_cb) = @_;
    return POE::Session->create(
        inline_states => {
            _start => sub {
                my ($kernel,$heap) = @_[KERNEL, HEAP];
                my $child = POE::Wheel::Run->new(
                    Program => $child_fn,
                    StdoutEvent => "update_cb",
                    StderrEvent => "_stderr_handler",
                    StdoutFilter => POE::Filter::Reference->new(),
                    StderrFilter => POE::Filter::Line->new(),
                );
                print "Launched child with pid " . $child->PID() . "\n";
                $_[KERNEL]->sig_child($child->PID(), "on_child_signal");
                $_[KERNEL]->sig(INT=>"child_cleanup", TERM => "child_cleanup");
                $_[HEAP]{cmd} = $child;
            },
            update_cb => $update_cb,
            _stop => sub { print "Stopping...\n"; },
            on_child_signal => sub {
                print "pid $_[ARG1] exited with status $_[ARG2]\n";
                delete $_[HEAP]{cmd};
            },
            child_cleanup => sub {
                my $child = $_[HEAP]{cmd};
                if($child) {
                    $child->kill(15);
                    print "Waiting for child " . $child->PID() . " to die\n";
                    wait;
                }
            },
			_stderr_handler => sub { print "$_[ARG0]\n" },
        },
    );
}

$0 .= " - Gearman Tester - ";

print "Launching $Options->{workers} workers\n";
for (1..$Options->{workers}) {
    launch_children(\&gearman_worker, mk_update_cb(\%worker_totals));
}
print "Launching $Options->{clients} clients\n";
for (1..$Options->{clients}) {
    launch_children(\&gearman_client, mk_update_cb(\%client_totals));
}

sub status_monitor {
}

sub print_progress {
    my ($kernel, $heap) = @_[KERNEL,HEAP];
    $|++;
    #print "[C/W (total/sec)]: ";
    #foreach (\%client_totals, \%worker_totals) {
    #    my $tmp = sum(values(%$_));
    #    next unless $tmp;
    #    printf("(%d %d)|", $tmp, $tmp / (time()-$begin_time));
    #}
    #print "\n";
    my $sum = sum(values(%client_totals));
    print "$sum\n" unless (!$sum);
    %client_totals = ();
    $kernel->delay("print_progress", UPDATE_INTERVAL);
    $|--;
}

print "Launching monitor..\n";
POE::Session->create(
    inline_states => {
        _start => sub {
            $0 .= " (MONITOR)";
            $_[HEAP]{last_update} = time();
            $_[KERNEL]->delay("print_progress", UPDATE_INTERVAL);
        },
        print_progress => \&print_progress,
    }
);

POE::Kernel->run();
