#!/usr/bin/perl

#TODO: fail_after_idle

package Gearman::Client;

our $VERSION;
$VERSION = '1.01';

use strict;
use IO::Socket::INET;
use Socket qw(IPPROTO_TCP TCP_NODELAY SOL_SOCKET);

use Gearman::Objects;
use Gearman::Task;
use Gearman::Taskset;
use Gearman::JobStatus;

sub new {
    my ($class, %opts) = @_;
    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{job_servers} = [];
    $self->{js_count} = 0;
    $self->{sock_cache} = {};

    $self->set_job_servers(@{ $opts{job_servers} })
        if $opts{job_servers};

    return $self;
}

sub new_task_set {
    my Gearman::Client $self = shift;
    return Gearman::Taskset->new($self);
}

# getter/setter
sub job_servers {
    my Gearman::Client $self = shift;
    unless (@_) {
        return wantarray ? @{$self->{job_servers}} : $self->{job_servers};
    }
    $self->set_job_servers(@_);
}

sub set_job_servers {
    my Gearman::Client $self = shift;
    my $list = ref $_[0] ? $_[0] : [ @_ ]; # take arrayref or array

    $self->{js_count} = scalar @$list;
    foreach (@$list) {
        $_ .= ":7003" unless /:/;
    }
    return $self->{job_servers} = $list;
}

sub _get_task_from_args {
    my Gearman::Task $task;
    if (ref $_[0]) {
        $task = $_[0];
        Carp::croak("Argument isn't a Gearman::Task") unless ref $_[0] eq "Gearman::Task";
    } else {
        my ($func, $arg_p, $opts) = @_;
        my $argref = ref $arg_p ? $arg_p : \$arg_p;
        Carp::croak("Function argument must be scalar or scalarref")
            unless ref $argref eq "SCALAR";
        $task = Gearman::Task->new($func, $argref, $opts);
    }
    return $task;

}

# given a (func, arg_p, opts?), returns either undef (on fail) or scalarref of result
sub do_task {
    my Gearman::Client $self = shift;
    my Gearman::Task $task = &_get_task_from_args;

    my $ret = undef;
    my $did_err = 0;

    $task->{on_complete} = sub {
        $ret = shift;
    };

    $task->{on_fail} = sub {
        $did_err = 1;
    };

    my $ts = $self->new_task_set;
    $ts->add_task($task);
    $ts->wait;

    return $did_err ? undef : $ret;

}

# given a (func, arg_p, opts?) or
# Gearman::Task, dispatches job in background.  returns the handle from the jobserver, or false if any failure
sub dispatch_background {
    my Gearman::Client $self = shift;
    my Gearman::Task $task = &_get_task_from_args;

    my ($jst, $jss) = $self->_get_random_js_sock;
    return 0 unless $jss;

    my $req = $task->pack_submit_packet("background");
    my $len = length($req);
    my $rv = $jss->write($req, $len);

    my $err;
    my $res = Gearman::Util::read_res_packet($jss, \$err);
    return 0 unless $res && $res->{type} eq "job_created";
    return "$jst//${$res->{blobref}}";
}

sub get_status {
    my Gearman::Client $self = shift;
    my $handle = shift;
    my ($hostport, $shandle) = split(m!//!, $handle);
    return undef unless grep { $hostport eq $_ } @{ $self->{job_servers} };

    my $sock = $self->_get_js_sock($hostport)
        or return undef;

    my $req = Gearman::Util::pack_req_command("get_status",
                                              $shandle);
    my $len = length($req);
    my $rv = $sock->write($req, $len);

    my $err;
    my $res = Gearman::Util::read_res_packet($sock, \$err);
    return undef unless $res && $res->{type} eq "status_res";
    my @args = split(/\0/, ${ $res->{blobref} });
    return undef unless $args[0];
    shift @args;
    $self->_put_js_sock($hostport, $sock);
    return Gearman::JobStatus->new(@args);
}

# returns a socket from the cache.  it should be returned to the
# cache with _put_js_sock.  the hostport isn't verified. the caller
# should verify that $hostport is in the set of jobservers.
sub _get_js_sock {
    my Gearman::Client $self = shift;
    my $hostport = shift;

    if (my $sock = delete $self->{sock_cache}{$hostport}) {
        return $sock if $sock->connected;
    }

    my $sock = IO::Socket::INET->new(PeerAddr => $hostport,
                                     Timeout => 1)
        or return undef;

    setsockopt($sock, IPPROTO_TCP, TCP_NODELAY, pack("l", 1)) or die;
    $sock->autoflush(1);
    return $sock;
}

# way for a caller to give back a socket it previously requested.
# the $hostport isn't verified, so the caller should verify the
# $hostport is still in the set of jobservers.
sub _put_js_sock {
    my Gearman::Client $self = shift;
    my ($hostport, $sock) = @_;

    $self->{sock_cache}{$hostport} ||= $sock;
}

sub _get_random_js_sock {
    my Gearman::Client $self = shift;
    my $getter = shift;
    return undef unless $self->{js_count};

    $getter ||= sub { my $hostport = shift; return $self->_get_js_sock($hostport); };

    my $ridx = int(rand($self->{js_count}));
    for (my $try = 0; $try < $self->{js_count}; $try++) {
        my $aidx = ($ridx + $try) % $self->{js_count};
        my $hostport = $self->{job_servers}[$aidx];
        my $sock = $getter->($hostport) or next;
        return ($hostport, $sock);
    }
    return ();
}

1;
__END__

=head1 NAME

Gearman::Client - Client for gearman distributed job system

=head1 SYNOPSIS

    use Gearman::Client;
    my $client = Gearman::Client->new;
    $client->job_servers('127.0.0.1', '10.0.0.1');

    # running a single task
    my $result_ref = $client->do_task("add", "1+2");
    print "1 + 2 = $$result_ref\n";

    # waiting on a set of tasks in parallel
    my $taskset = $client->new_task_set;
    $taskset->add_task( "add" => "1+2", {
       on_complete => sub { ... }
    });
    $taskset->add_task( "divide" => "5/0", {
       on_fail => sub { print "divide by zero error!\n"; },
    });
    $taskset->wait;


=head1 DESCRIPTION

I<Gearman::Client> is a client class for the Gearman distributed job
system, providing a framework for sending jobs to one or more Gearman
servers.  These jobs are then distributed out to a farm of workers.

Callers instantiate a I<Gearman::Client> object and from it dispatch
single tasks, sets of tasks, or check on the status of tasks.

=head1 USAGE

=head2 Gearman::Client->new(%options)

Creates a new I<Gearman::Client> object, and returns the object.

If I<%options> is provided, initializes the new client object with the
settings in I<%options>, which can contain:

=over 4

=item * job_servers

Calls I<job_servers> (see below) to initialize the list of job
servers.  Value in this case should be an arrayref.

=back

=head2 $client->job_servers(@servers)

Initializes the client I<$client> with the list of job servers in I<@servers>.
I<@servers> should contain a list of IP addresses, with optional port
numbers. For example:

    $client->job_servers('127.0.0.1', '192.168.1.100:7003');

If the port number is not provided, C<7003> is used as the default.

=head2 $client-E<gt>do_task($task)

=head2 $client-E<gt>do_task($funcname, $arg, \%options)

Dispatches a task and waits on the results.  May either provide a
L<Gearman::Task> object, or the 3 arguments that the Gearman::Task
constructor takes.

Returns a scalar reference to the result, or undef on failure.

If you provide on_complete and on_fail handlers, they're ignored, as
this function currently overrides them.

=head2 $client-E<gt>dispatch_background($task)

=head2 $client-E<gt>dispatch_background($funcname, $arg, \%options)

Dispatches a task and doesn't wait for the result.  Return value
is an opaque scalar that can be used to refer to the task.

=head2 $taskset = $client-E<gt>new_task_set

Creates and returns a new I<Gearman::Taskset> object.

=head2 $taskset-E<gt>add_task($task)

=head2 $taskset-E<gt>add_task($funcname, $arg, $uniq)

=head2 $taskset-E<gt>add_task($funcname, $arg, \%options)

Adds a task to a taskset.  Three different calling conventions are
available.

=head2 $taskset-E<gt>wait

Waits for a response from the job server for any of the tasks listed
in the taskset. Will call the I<on_*> handlers for each of the tasks
that have been completed, updated, etc.  Doesn't return until
everything has finished running or failing.

=head1 EXAMPLES

=head2 Summation

This is an example client that sends off a request to sum up a list of
integers.

    use Gearman::Client;
    use Storable qw( freeze );
    my $client = Gearman::Client->new;
    $client->job_servers('127.0.0.1');
    my $tasks = $client->new_task_set;
    my $handle = $tasks->add_task(sum => freeze([ 3, 5 ]), {
        on_complete => sub { print ${ $_[0] }, "\n" }
    });
    $tasks->wait;

See the I<Gearman::Worker> documentation for the worker for the I<sum>
function.

=cut
