#!/usr/bin/perl

#the following begin block allows use to make a pseudo-module named _Constants
#and use it in our code via the fake '_Constants.pm' file.. we need a BEGIN
#here to modify the %INC hash

BEGIN {
    use strict;
    use warnings;
    package _Constants;
    use base qw(Exporter);
    our @EXPORT = qw(_FAILED _SUCCESS _INPROGRESS randstring);
    use List::Util "shuffle";
    use constant _FAILED => -1;
    use constant _INPROGRESS => 0;
    use constant _SUCCESS => 1;
    sub randstring {
        my $length = $_[0];
        my @l = ("a".."z", "A".."Z");
        return join("", (shuffle(@l))[0..$length-1]);
    }
    $INC{"_Constants.pm"} = 1;
}

#this wraps a Gearman::Task object, so that we can get a more accurate understanding
#of whether a task completed successfully or not
package _TaskWrapper;
use strict;
use warnings;
use base "Gearman::Task";
use _Constants;
#Gearman::Task uses the fields module, and thus in order to extend the class,
#we need to use fields as well
use fields qw(status);
sub new {
    my ($cls, $qname, $arg, $opts) = @_;
    my $self = fields::new($cls);
    $self->status(_INPROGRESS);
    $self->SUPER::new($qname,$arg,$opts);
    return $self;
}
#gets/sets an integer constant representing the current status
sub status {
    my $self = shift;
    my $new = shift;
    if (defined $new) { $self->{status} = $new }
    return $self->{status};
}

#the following two are hacks, but we need to hook into this calls in order to
#determine properly whether our server has failed or not. These are called
#internally by Gearman::Task upon completion/failure
sub complete {
    my $self = shift;
    $self->status(_SUCCESS);
    return $self->SUPER::complete(@_);
}

sub final_fail {
    my $self = shift;
    $self->status(_FAILED);
    return $self->SUPER::final_fail(@_);
}

#just a context manager for us.. provides two distinct pieces of functionality
#it allows us to test a jobserver by setting up a dummy worker and spamming it
#with jobs, and it also allows us to fetch the status of individual queues in
#a hash
=head1 DESCRIPTION

Gearman::Healthtest is a module that allows one to test the status of a given
gearman server. It requires UNIVERSAL::ref as a dependency, and must be B<use>d
before any Gearman:: modules

=head1 SYNOPSIS

    my $tester = Gearman::Healthtest->new(
    "jobserver:4730", $timeout, $test_queue_name);
    my ($njobs, $time_taken_to_complete) = $tester->test();
    my %status_hash = $tester->getstatus(newsock=>1, timeout=> $timeout);

=cut

package _Util;
use IO::Socket::INET;
use Data::Dumper;
sub force_so_linger {
    my $fn = shift;
    my $args = \@_;
    {
        #this is a weird hack, but basically what this does is force an
        #SO_LINGER on the socket. what this means is that close() will wait
        #until the socket is actually close rather than backgrounding it.
        #this is important to us if we are going to be making this call
        #frequently.
        my $real_newsock = \&IO::Socket::INET::new;
        no warnings;
        local *IO::Socket::INET::new = sub {
            my $ret = $real_newsock->(@_);
            return $ret if !defined $ret;
            setsockopt($ret, SOL_SOCKET, SO_LINGER, pack("ii", (1, 0)));
            return $ret;
        };
        use warnings;
        return $fn->(@$args);
    }
}

package Gearman::Healthtest;
use _Constants;
use strict;
use warnings;
use Gearman::Client;
use Gearman::Worker;
use IO::Socket::INET;
use POSIX;
use Hash::Util qw(lock_keys unlock_keys);
use Time::HiRes qw(sleep time);
use constant MAXTASKS => 200;
use constant DEFAULT_DATALEN => 4096;
use Data::Dumper;

#fields, because i suck at typing and still need to learn something like Moose
#or whatever
my @__fields = ();
=head2 METHODS

=over

=item B<< Gearman::Healthtest->new() >>

initialize a new tester object, providing $jobserver, $timeout, $queue_name
as arguments, as well as a hash of other optional arguments:
    C<$jobserver>: e.g. "foo.bar.com:4730"
    C<$timeout>: timeout in seconds. this can be a floating point number
    C<$queue_name>: the name of the queue we can test in gearman.
        there is no default, so make sure you choose something which doesn't
        conflict with a real queue.

optional keyword arguments currently are
    C<datalen>: the length of sample data (actually a bunch of null bytes)
    to exchange between the client and worker.
    This currently defaults to 4096 if not specified

In the background this actually forks and creates a worker which registers with
the jobserver, then spams it (via the jobserver) with requests.
=cut
push @__fields, (qw(jobserver timeout queue_name client worker testdata));
sub new {
    my $cls = shift;
    my $self = {};
    #can't bless a locked hash, so bless it ASAP
    bless($self, $cls);
    lock_keys(%$self, @__fields);
    
    #positional arguments...
    $self->{$_} = shift @_ foreach qw(jobserver timeout queue_name);
    my (%opts) = @_;
    $self->{testdata} = \scalar("I" x scalar((defined $opts{datalen}) ?
        $opts{datalen} : DEFAULT_DATALEN));
        
    
    $self->{client} = Gearman::Client->new();
    $self->{client}->job_servers($self->{jobserver});            
    return $self;
}

sub _do_worker {
    $|++;
    my $self = shift;
    my $pid = fork();
    my $begin_time = time();
    my $time_remaining = $self->{timeout};
    if($pid > 0) {return $pid;}
    elsif($pid < 0) { warn "$!"; return}
    my $stop_work = 0;
    my @sockets;
    my $worker = Gearman::Worker->new();
    $worker->job_servers($self->{jobserver});
    $worker->register_function($self->{queue_name}, sub { $self->{testdata} });
    $SIG{USR1} = sub { exit(1); };
    
    while(!$stop_work && $time_remaining > 0) {
        $time_remaining -= (time() - $begin_time);
        $worker->work(stop_if => sub { $stop_work || $time_remaining <= 0});
    }
    print "Exiting\n";
    exit(0);
}

=item B<< $tester->test() >>

test the server. This will submit 200 jobs or as many jobs as it can before the
$timeout (passed to L</new()>). Returns an arrayref of [$njobs,$duration]

you can optionally pass a maximum amount of jobs. the default is 200

=cut
sub test {
    my ($self) = shift;
    my $maxtasks = shift || MAXTASKS;
    my $completed = 0;
    #hook up everything...
    my $worker_pid = _Util::force_so_linger(
                    $self->can("_do_worker"), $self) || return;
    #do as many jobs as we can cram into our time interval
    my $begin_time = time();
    my $duration;
    my %tsks = ();
    my $time_remaining = $self->{timeout};
    while($time_remaining >= 0 && scalar(keys %tsks) < $maxtasks) {
        $time_remaining = -(time() - $begin_time - $self->{timeout});
        my $task = _TaskWrapper->new(
            $self->{queue_name}, $self->{testdata}, {timeout=> $time_remaining});
        eval {
            _Util::force_so_linger(
                $self->{client}->can('do_task'), $self->{client}, $task);
        };
        if($@) {
            warn "$!" if $!;
            warn "Got Error: $@\n";
            last;
        }
        
        if($task->status == _SUCCESS) { $completed++ }
        $tsks{$task} = $task;
        #can't find a sane async interface for this.. oh well...
    }
    $duration = time() - $begin_time;
    kill(SIGUSR1, ($worker_pid));
    waitpid($worker_pid, 0);
    return [$completed, $duration];
}


####################### STATUS REPORT #####################

#status sock is the persistent socket used for querying the status
#the _status_cts is a boolean value which tells us if we have received a fully
#qualified (e.g. \n. terminated) message on our last read. We will set an
push @__fields, (qw(_status_sock _status_cts));


#gets a socket (either by using an existing socket, or creating a new one.
#set newsock => 1 to force creation of a new socket
sub _get_status_sock {
    my $self = shift;
    my %opts = @_;
    
    if($self->{_status_sock}) {
        if($opts{newsock}) {
            close($self->{_status_sock});
            $self->{_status_sock} = undef;
        } else {
            return $self->{_status_sock};
        }
    }
    my $jobserver = ($self->{jobserver});
    my ($host, $port) = split(":", $jobserver);
    if(!defined $port) { $port = 4730 }
    my $sock = IO::Socket::INET->new(
        PeerHost => $host,
        PeerPort => $port,
        ReuseAddr => 1,
        Blocking => 0,
        Timeout => $opts{timeout},
    );
    return if !defined $sock;
    
    setsockopt($sock, SOL_SOCKET, SO_LINGER, pack("ii", 1, 0));
    
    $self->{_status_sock} = $sock;
    $self->{_status_cts} = 1;
    return $sock;
}

#tries to get a status dump from gearman's administrative interface
sub _sock_io {
    my $self = shift;
    my $timeout = shift;
    my $delim_regexp = qr/^[.]\n\Z/m;
    my $sendbuf = "status\n";
    my $begin_time = time();
    my $time_remaining = $timeout;
    my $io_wrap = sub {
        $time_remaining -= (time() - $begin_time);
        my $result = shift;
        #none of these exceptional conditions, except EAGAIN, should happen
        #under normal operation.. 
        if(!defined $result && $! != EAGAIN) {
            warn "SOCKET ERROR!";
            goto ERR;
        } elsif ($! == EAGAIN) {
            #nothing to do here...
        } elsif ($result == 0) {
            warn "SOCKET CLOSED!";
            goto ERR;
        }
        return $result;
    };
    my $s = $self->{_status_sock};
    #send
    while($time_remaining > 0 && $sendbuf) {
        my $nsent = $io_wrap->(syswrite($s, $sendbuf));
        $sendbuf = substr($sendbuf, $nsent);
    }
    #receive
    #we can be crazy optimizing here and try not to recheck the entire string
    #each time, but really, we're not expecting heavy traffic here..
    my $match_found = 0;
    my $rbuf = "";
    while($time_remaining > 0 && !$match_found) {
        $io_wrap->(sysread($s, my $tmp, 4096));
        $rbuf .= $tmp;
        if($tmp =~ $delim_regexp || $rbuf =~ $delim_regexp) {
            $match_found = 1;
        }
    }
    return $rbuf unless !$match_found;
    ERR:
    $self->_get_status_sock(timeout => $timeout, newsock => 1);
    return;
}

=item B<< $tester->getstatus(timeout => $self->{timeout}, newsock => 0) >>

Returns status information from Gearman's administrative (text-based) interface.
This will try to connect to a gearman port, and wait $timeout seconds for status
information. Optionally you may set newsock to 1 in order to force a new socket
on every fetch. Otherwise it will keep an internal socket object in state and try
to use that. The return information is in a hash keyed by queue name, and whose
values are an arrayref in the format of [nwaiting, nworking, navailable]
=cut
sub getstatus {
    my $self = shift;
    my %opts = @_;
    $opts{timeout} = ($opts{timeout}) ? $opts{timeout} : $self->{timeout};
    #initialize the socket...
    my $status = $self->_get_status_sock(%opts);
    warn "$!" if $!;
    return if !defined $status;
    
    my $ret = $self->_sock_io($opts{timeout});
    my %h = ();
    if(!defined $ret) { return }
    foreach my $l (split("\n", $ret)) {
        my ($q, $nwait, $nwork, $navail) = split("\t", $l);
        next if(!defined $navail);
        $h{$q} = [$nwait, $nwork, $navail];
    }
    return %h;
}

=item B<< test_multi($host,$timeout, maxjobs => 300, batchsize => 50) >>

This is a static method. It creates multiple test objects, and tests repeatedly
until the timeout expires or a set amount of jobs have been executed

maxjobs is the maximum amount of jobs to wait for, batchsize is the amount of
jobs to run for each client

=cut

sub test_multi {
	my ($host,$timeout, $queue_name, %opts) = @_;
	$host .= ":4730" unless $host =~ /:\d+$/;
	my $begin_time = time();
	my $time_remaining = $timeout;
	my %params = (maxjobs => 300, batchsize => 50, %opts);
	my $completed = 0;
	while($time_remaining >= 0 && $completed < $params{maxjobs}) {
		$time_remaining = -(time() - $begin_time - $timeout);
		my $tester = __PACKAGE__->new($host, 1, $queue_name);
		my ($ndone,undef) = @{ $tester->test($params{batchsize}) };
		$completed += $ndone if $ndone;
	}
	return $completed;
}

if(!caller) {
    $|++;
    my $test = Gearman::Healthtest->new("localhost:4730", 1, "foo", datalen=>1);
    my $result = $test->test(1);
    if(defined $result) {
        my ($completed,$time) = @$result;
        print "Got $completed jobs in $time seconds\n";
    } else {
        print "OOPS!\n";
    }
    my %ret = $test->getstatus();
    foreach (keys %ret) {
        my $qname = $_;
        my ($nwait,$nwork,$navail) = @{$ret{$_}};
        printf("Queue: %-15s [WAIT %-4d] [WORK %-4d] [AVAIL %-4d]\n",
               $qname, $nwait, $nwork, $navail);
    }
}

1;
