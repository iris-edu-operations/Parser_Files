#!/usr/bin/env perl

# A script to watch data extraction diagnostic messages

use strict;
use Getopt::Long;
use File::Basename;
use Net::AMQP::RabbitMQ;
use JSON::PP;
use Term::ANSIColor;
use List::Util qw(min);

my $version = "2016.224";
my $verbose = 0;
my $usage;

my $noheader = 0;
my $header_interval = 20;

# Block sizes
my $block_size = "B";
my %block_sizes = (

		"B" => 1,

		"K"  => 1000,
		"KB" => 1024,

		"M"  => 1000 * 1000,
		"MB" => 1024 * 1024,

		"G"  => 1000 * 1000 * 1000,
		"GB" => 1024 * 1024 * 1024,
	);

# Stats
my $bin_span_sec = 60;
my $bin_num_max = 1440;
my $stats_hash_ref = {};
my $log_date_format = "%Y-%m-%d %H:%M:%S %Z";
my $prev_bin_key = ""; 

my $useragent;
my $error;
my $errorWOnodata;
my $hostmatch;
my $clientip;
my $minsearch;

my $outputformat = "timing";

my $host = "broker-int";
my $port = 5672;
my $user = "irisrabbit";
my $pass = "eel8ed";

my $vhost = "services";
my $exchange = "transient_stats";
my $routing_key = "transient.stats.dataextraction";

my $terminate = 0;
$SIG{INT} = sub { $terminate = 1; };
$SIG{TERM} = sub { $terminate = 1; };

# Parse command line arguments
Getopt::Long::Configure(qw{ bundling_override no_ignore_case_always });
my $getoptsret = GetOptions ( 'verbose|v+'     => \$verbose,
                              'noheader|nh'    => \$noheader,
                              'usage|h'        => \$usage,
                              'block-size|b=s' => \$block_size,

                              'TO'             => sub { $outputformat = "timing"; },
                              'RO'             => sub { $outputformat = "request"; },
                              'DO'             => sub { $outputformat = "detailed"; },
                              'json|j'         => sub { $outputformat = "json"; },
                              'ST'             => sub { $outputformat = "stats"; },
                              'useragent|UA=s' => \$useragent,
                              'error|e'        => \$error,
                              'E'              => \$errorWOnodata,
                              'hostmatch|H=s'  => \$hostmatch,
                              'clientip|c=s'   => \$clientip,
                              'minsearch|ms=i' => \$minsearch,
                            );

if ( ! $getoptsret || defined $usage || not defined $block_sizes{$block_size} ) {
  my $name = basename ($0);
  print STDERR <<EOM;
Watch data extraction diagnostic messages ($version)

Usage: $name [OPTIONS]

  --usage,-h                   Print this help message
  --verbose,-v                 Be more verbose, multiple flags can be used

  --useragent,-UA <match>      Limit to messages with matching UserAgent
  --error,-e                   Limit to messages with errors
  --E                          Limit to messages with errors, but exclude "no data" errors
  --host,-H  <host>            Limit to messages from matching host
  --clientip,-c  <IP>          Limit to messages from matching client IP
  --minsearch,-ms <sec>        Limit to messages with a minimum data search time
  --block-size,-b <SIZE>       Print request size as in 'ls' using SIZE-byte blocks. See SIZE format below.

  SIZE may be (or may be an integer optionally followed by) one of following: B for 1, KB for 1000, K for 1024, MB for 1000*1000, M for 1024*1024, and so on for G (T,P,E,Z,Y not supported).

  Output formats:

  -TO          Timing-oriented information, one line per message (default)
  -RO          Request-oriented information, one line per message
  -DO          Detailed output, all details with descriptions
  --json,-j    JSON-formatted output, pretty-printed for readability

EOM
  exit (1);
}

my $mq = Net::AMQP::RabbitMQ->new();

# Connect to message broker, open channel, declare queue and bind to exchange with routing key
$mq->connect($host, { vhost => $vhost,
                      user => $user,
                      password => $pass,
                      heartbeat => 60 });
$mq->channel_open(1);
my $queuename = $mq->queue_declare(1, "", { exclusive => 1 });
$mq->queue_bind(1, $queuename, $exchange, $routing_key);

my $serverprops = $mq->get_server_properties();
my $clientprops = $mq->get_client_properties();

if ( $verbose ) {
  printf "Connected to messaging system %s (%s) on $host with %s (%s)\n",
         $serverprops->{product}, $serverprops->{version},
         $clientprops->{product}, $clientprops->{version};
}

$mq->consume(1, $queuename, { consumer_tag => "$0" });
my $heartbeattime = time;
my $headercount = 0;

print "Entering main loop, format is '$outputformat'\n" if ( $verbose );
print "Stats bin size is $bin_span_sec seconds \n" if ( $verbose and ( $outputformat eq "stats" ) );

while ( ! $terminate )
  {
    my $looptime = time;

    if ( my $message = $mq->recv(100) ) {
      #use Data::Dumper;
      #print Dumper($message);

      my $messref = decode_json $message->{body};
      &HandleMessage ($messref);

      $heartbeattime = $looptime;
    }
    elsif ( ! $terminate && (time - $heartbeattime) >= 30 ) {
      print "Sending heartbeat\n" if ( $verbose >= 2 );
      $mq->heartbeat();
      $heartbeattime = $looptime;
    }
  }

# Disconnect
$mq->disconnect();

## End of main

# ----------------------------------------------
sub HandleMessage { # HandleMessage (hashref)

  my $messref = shift;

  # Message filters match useragent, host, clientip.
  return if ( $useragent && $messref->{userAgent} !~ /$useragent/i );
  return if ( $hostmatch && $messref->{host} !~ /$hostmatch/i );
  return if ( $clientip  && $messref->{clientIP} !~ /$clientip/i );

  # Stats handles error differently - allows them through and counts them.
  if ( $outputformat ne "stats" ) {

  	if ( $errorWOnodata ) {
    	return if ( ! exists $messref->{error} );
    	return if ( $messref->{exitValue} == 2 );  # The WSS-handler exit value of 2 means "no data"
  	} elsif ( $error ) {
    	return if ( ! exists $messref->{error} );
  	}
  }

  # Message filter on total data search time less than a minimum.
  return if ( $minsearch && ! exists $messref->{totalDataSearchSeconds} );
  return if ( $minsearch && $messref->{totalDataSearchSeconds} < $minsearch );

  # Create short host name (e.g. wslive1.iris, wslive4.adc1)
  my @hostparts = split (/\./, $messref->{host});
  my $shorthost = $hostparts[0];
  $shorthost .= ".$hostparts[1]" if ( $hostparts[1] );

  # Create short version of UserAgent, just the first part
  my ($shortua) = split (/ /, $messref->{userAgent});

  # Trim sub-second zeros from request time
  $messref->{requestTime} =~ s/\.0+Z$/Z/;

  # Generate output according to type requested
  if ( $outputformat eq "timing" ) {

    # Print a header every N messages
    if ( $noheader == 0 and $headercount == 0 ) {
      print "Host        , Client IP     , Request time        , Req  , Slect, MDsrch, RstChk, DBconn, TSsrch,TSrow,AStat,DSrch, Time, Size     , UserAgent, ExitValue\n";
      #      wslive4.iris,  192.168.168.1, 2016-08-11T23:25:10Z,   750,   162, 0.0422, 0.0090, 0.0020, 0.5014,  162, 6.41, 0.52,    1,  10.3 MiB, FetchData/2016.089
      $headercount = $header_interval;
    }
    else {
      $headercount--;
    }

    printf ("%12.12s, %14.14s, %20.20s, %5.5s, %5.5s",
            $shorthost,
            $messref->{clientIP},
            $messref->{requestTime},
            $messref->{requestCount},
            $messref->{requestChannelCount},
           );

    printf (", %4.4f", $messref->{metadataSearchSeconds});
    printf (", %4.4f", $messref->{restrictedCheckSeconds});
    printf (", %4.4f", $messref->{databaseConnectSeconds});
    printf (", %4.4f", $messref->{timeseriesIndexSearchSeconds});
    printf (", %4.4s", $messref->{timeseriesIndexRowCount});
    printf (", %4.4s", $messref->{archiveFileStatSeconds});
    printf (", %4.4s", $messref->{totalDataSearchSeconds});
    printf (", %4.4s", $messref->{totalRunTimeSeconds});
    printf (", %11s", sizestring($block_size, $messref->{shippedByteCount}));
    
    # Modify Chad to add exitValue
    # printf (", $shortua\n");
    printf (", $shortua");
    printf (", %2s\n", $messref->{exitValue} );

  } elsif ( $outputformat eq "request" ) {

    # Print a header every N messages
    if ( $noheader == 0 and $headercount == 0 ) {

      print "Host        , Client IP     , Request time        , Req  , Slect, Earliest request   , Latest request     , RSec, Time, Size     , UserAgent, ExitValue\n";
      #      wslive4.iris,   128.95.16.19, 2016-08-12T00:00:22Z,     1,     1, 2016-08-10T18:56:34, 2016-08-10T19:57:36, 3662,    0, 273.0 KiB, ObsPy
      $headercount = $header_interval;

    } else {

      $headercount--;

    }

    printf ("%12.12s, %14.14s, %20.20s, %5.5s, %5.5s",
            $shorthost,
            $messref->{clientIP},
            $messref->{requestTime},
            $messref->{requestCount},
            $messref->{requestChannelCount},
           );

    printf (", %19.19s", $messref->{requestEarliest});
    printf (", %19.19s", $messref->{requestLatest});
    printf (", %4.4s", $messref->{requestCoverageSeconds});

    printf (", %4.4s", $messref->{totalRunTimeSeconds});
    printf (", %11s", sizestring($block_size, $messref->{shippedByteCount}));
    
    # Modify Chad to add exitValue
    # printf (", $shortua\n");
    printf (", $shortua");
    printf (", %2s\n", $messref->{exitValue} );

  } elsif ( $outputformat eq "detailed" ) {

    print "== Run Report ==\n";
    print "  Host: $messref->{host}\n";
    print "  Client IP: $messref->{clientIP}\n";
    print "  UserAgent: $messref->{userAgent}\n";
    print "  Request time: $messref->{requestStart}\n";
    print "  Request URL: $messref->{requestURL}\n";
    print "  Request count: $messref->{requestCount}\n";
    print "  Request earliest: $messref->{requestEarliest}, latest: $messref->{requestLatest}, coverage: $messref->{requestCoverageSeconds}\n";
    print "\n";
    print "  Metadata search (seconds): $messref->{metadataSearchSeconds}\n";
    print "  Restricted check (seconds): $messref->{restrictedCheckSeconds}\n";
    print "  Request channels selected: $messref->{requestChannelCount}\n";
    print "\n";
    print "  Database connect (seconds): $messref->{databaseConnectSeconds}\n";
    print "  Timeseries Index search (seconds): $messref->{timeseriesIndexSearchSeconds}, rows: $messref->{timeseriesIndexRowCount}\n";
    print "  Archive file stat time (seconds): $messref->{archiveFileStatSeconds}\n";
    print "  Archive file search (seconds): $messref->{archiveSearchSeconds}, files: $messref->{archiveFileCount}\n";
    print "  BUD file search (seconds): $messref->{budSearchSeconds}, files: $messref->{budFileCount}\n";
    print "  Total data search (seconds): $messref->{totalDataSearchSeconds}\n";
    print "\n";
    print "  Data extraction (seconds): $messref->{extractionSeconds}\n";
    print "\n";
    print "  Total run time (seconds): $messref->{totalRunTimeSeconds}\n";
    print "  Shipped bytes: $messref->{shippedByteCount}, shipped channels: $messref->{shippedChannelCount}\n";
    print "  Exit value: $messref->{exitValue}\n" if ( $messref->{exitValue} );
    print "  ERROR: $messref->{error}\n" if ( $messref->{error} );

  } elsif ( $outputformat eq "json" ) {

    #print "== JSON Message ==\n";
    my $coder = JSON::PP->new->pretty(1)->sort_by(sort());
    print $coder->encode ($messref);

  } elsif ( $outputformat eq "stats" ) {

	# Define keys for stats to store.
	my $key_all_count = 'count';
	my $key_no_data_count = 'count_no_data';
	my $key_error_count = 'count_error';
	my $key_byte_count = 'count_shipped_bytes';
    my $key_dbconn_times = 'dbconn_time_msec';
    my $key_mdsearch_times = 'mdsearch_time_msec';
    my $key_req_times = 'req_time_sec';

	# Get bin key for record.
	my $req_beg = $messref->{requestTimeEpoch};

	# Get metadata search time for record.
	my $mdsearch_time = $messref->{metadataSearchSeconds};
	# Not sure when this case happens but we use 0 for it.
	if ( $mdsearch_time eq "" ) {
		# print "mdsearch_time = [$mdsearch_time] \n";
		$mdsearch_time = 0;
	}

	# Get db connection time for record.
	my $dbconn_time = $messref->{databaseConnectSeconds};
	# Not sure when this case happens but we use 0 for it.
	if ( $dbconn_time eq "" ) {
		# print "dbconn_time = [$dbconn_time] \n";
		$dbconn_time = 0;
	}

	# Get request time for record.
	my $req_time = $messref->{totalRunTimeSeconds};
	# Not sure when this case happens but we use 0 for it.
	if ( $req_time eq "" ) {
		# print "req_time = [$req_time] \n";
		$req_time = 0;
	}

	my $req_end = $req_beg + $req_time;
	my $bin_start_time = $bin_span_sec * int($req_end / $bin_span_sec);
	my $bin_key = $bin_start_time; 

	# print "bin key = $bin_key for $req_end when bin span = $bin_span_sec\n";

	# Get stats hash for bin key else create it.
	# REF for reference to hash = http://perldoc.perl.org/perlreftut.html
	my $bin_hash_ref = $stats_hash_ref->{ $bin_key };
	if ( not defined $bin_hash_ref ) {

		# We have a new bin - make room if hash is maxed out by deleting oldest bin key.
		my $stats_hash_size = keys %$stats_hash_ref;
		if ( $stats_hash_size >= $bin_num_max ) {

			my $min_key = min keys %$stats_hash_ref;
			#print "HHHHHH hash-size = $stats_hash_size min = $min_key \n";
			
			delete $stats_hash_ref->{ $min_key };
		}

		# print "HASH $bin_key NOT exists - CREATING! \n";
		# Create and save stats hash against bin key.
		$bin_hash_ref = {};
		$stats_hash_ref->{ $bin_key } = $bin_hash_ref;

		# Print results for prev hash.
		# NOTE - if events came out of sequence then this could summarize a bin which later gets updated by an out of sequence record!
		if ( $prev_bin_key ne "" ) {

			#print "PRINTING HASH for $prev_bin_key! \n";

			# Get previous bin hash.
			my $prev_bin_hash_ref = $stats_hash_ref->{ $prev_bin_key };

			if ( defined $prev_bin_hash_ref ) {

				# Generate stats for previous bin.
				my $all_count = $prev_bin_hash_ref->{ $key_all_count };
				my $no_data_count = $prev_bin_hash_ref->{ $key_no_data_count };
				my $error_count = $prev_bin_hash_ref->{ $key_error_count };
				my $byte_count = $prev_bin_hash_ref->{ $key_byte_count };

				# Prepare metadata search times stats.
				my $mdsearch_times_ref = $prev_bin_hash_ref->{ $key_mdsearch_times };
				my @mdsearch_times_sorted = sort {$a <=> $b} @$mdsearch_times_ref;
				my $mdsearch_times_size = @mdsearch_times_sorted;
				my $mdsearch_times_formatted = sprintf("[%4s,%4s,%4s,%4s]", 
					$mdsearch_times_sorted[ int(0.50 * ($mdsearch_times_size - 1)) ],
					$mdsearch_times_sorted[ int(0.90 * ($mdsearch_times_size - 1)) ],
					$mdsearch_times_sorted[ int(0.95 * ($mdsearch_times_size - 1)) ],
					$mdsearch_times_sorted[            ($mdsearch_times_size - 1) ] 
				 	);

				# Prepare db connection times stats.
				my $dbconn_times_ref = $prev_bin_hash_ref->{ $key_dbconn_times };
				my @dbconn_times_sorted = sort {$a <=> $b} @$dbconn_times_ref;
				my $dbconn_times_size = @dbconn_times_sorted;
				my $dbconn_times_formatted = sprintf("[%4s,%4s,%4s,%4s]", 
					$dbconn_times_sorted[ int(0.50 * ($dbconn_times_size - 1)) ],
					$dbconn_times_sorted[ int(0.90 * ($dbconn_times_size - 1)) ],
					$dbconn_times_sorted[ int(0.95 * ($dbconn_times_size - 1)) ],
					$dbconn_times_sorted[            ($dbconn_times_size - 1) ] 
				 	);

				# Prepare request times stats.
				my $req_times_ref = $prev_bin_hash_ref->{ $key_req_times };
				my @req_times_sorted = sort {$a <=> $b} @$req_times_ref;
				my $req_times_size = @req_times_sorted;
				my $req_times_formatted = sprintf("[%4s,%4s,%4s,%4s]", 
					$req_times_sorted[ int(0.50 * ($req_times_size - 1)) ],
					$req_times_sorted[ int(0.90 * ($req_times_size - 1)) ],
					$req_times_sorted[ int(0.95 * ($req_times_size - 1)) ],
					$req_times_sorted[            ($req_times_size - 1) ]
				 	);

				# print "YYYYYYYYYY $req_time_bins_formatted  $req_times_formatted for  [ @req_times_sorted ] \n";
			
    			# Print a header every N messages
    			if ( $noheader == 0 and $headercount == 0 ) {
					print "bin_key, num_req, num_nodata, num_error, byte_count, mdsea_msec_50_90_95_max, dbconn_msec_50_90_95_max, req_sec_50_90_95_max \n";
      				$headercount = $header_interval;
				} else {
      				$headercount--;
				}

				# Print stats.
				#
				my $formatted_bin_key = ` date +\"$log_date_format\" --date='\@$prev_bin_key' `;
				chomp( $formatted_bin_key );

    			printf ("%s", $formatted_bin_key);
    			printf (",%5d", $all_count);
    			printf (",%5d", $no_data_count);
    			printf (",%5d", $error_count);
    			printf (",%11s", sizestring($block_size, $byte_count));
    			printf (",%10s", $dbconn_times_formatted);
    			printf (",%10s", $mdsearch_times_formatted);
    			printf (",%10s", $req_times_formatted);
				printf ("\n");
			}
		}
    }

	# $bin_hash_ref = $stats_hash_ref->{ $bin_key };

	# Update the stats hash.

	# Update request count.
	if ( not exists $bin_hash_ref->{ $key_all_count } ) {
		$bin_hash_ref->{ $key_all_count } = 0;
	} else {
		$bin_hash_ref->{ $key_all_count } += 1;
	}

	# Update shipped byte count.
	if ( not exists $bin_hash_ref->{ $key_byte_count } ) {
		$bin_hash_ref->{ $key_byte_count } = 0;
	} else {
		$bin_hash_ref->{ $key_byte_count } += $messref->{shippedByteCount};
	}

	# Update error count.
	if ( not exists $bin_hash_ref->{ $key_error_count } ) {

		$bin_hash_ref->{ $key_error_count } = 0;

	} elsif ( exists $messref->{error} ) {

		# Update if we are not excluding no-data errors, or if we are and it was not a no-data error.
  		# The WSS-handler exit value of 2 means "no data"
  		if ( ! $errorWOnodata or $messref->{exitValue} != 2 ) {

			$bin_hash_ref->{ $key_error_count } += 1;
		}
	}

	# Update no data count.
	if ( not exists $bin_hash_ref->{ $key_no_data_count } ) {

		$bin_hash_ref->{ $key_no_data_count } = 0;

	} elsif ( exists $messref->{error} and $messref->{exitValue} == 2 ) {

  		# The WSS-handler exit value of 2 means "no data"
		$bin_hash_ref->{ $key_no_data_count } += 1;
	}

	# Update metadata search times - save as msec.
	$mdsearch_time = int( 1000.0 * $mdsearch_time);
	if ( not exists $bin_hash_ref->{ $key_mdsearch_times } ) {

		# Save array of times for the bin as an array ref
		my @mdsearch_times = ( $mdsearch_time );
		$bin_hash_ref->{ $key_mdsearch_times } = \@mdsearch_times;

	} else {

		# Retrieve array ref as a scale variable
		my $mdsearch_times_ref = $bin_hash_ref->{ $key_mdsearch_times };

		# Push the value onto the dereferenced array ref.
	 	push( @$mdsearch_times_ref, $mdsearch_time );
	}

	# Update db connection times - save as msec.
	$dbconn_time = int( 1000.0 * $dbconn_time);
	if ( not exists $bin_hash_ref->{ $key_dbconn_times } ) {

		# Save array of times for the bin as an array ref
		my @dbconn_times = ( $dbconn_time );
		$bin_hash_ref->{ $key_dbconn_times } = \@dbconn_times;

	} else {

		# Retrieve array ref as a scale variable
		my $dbconn_times_ref = $bin_hash_ref->{ $key_dbconn_times };

		# Push the value onto the dereferenced array ref.
	 	push( @$dbconn_times_ref, $dbconn_time );
	}

	# Update request times.
	if ( not exists $bin_hash_ref->{ $key_req_times } ) {

		# Save array of times for the bin as an array ref
		my @req_times = ( $req_time );
		$bin_hash_ref->{ $key_req_times } = \@req_times;

	} else {

		# Retrieve array ref as a scale variable
		my $req_times_ref = $bin_hash_ref->{ $key_req_times };

		# Push the value onto the dereferenced array ref.
	 	push( @$req_times_ref, $req_time );
	}

	$prev_bin_key = $bin_key;

  } else {

    die "Unrecognized output format '$outputformat'\n";
  }
}

######################################################################
# sizestring (block_size, bytes):
#
# Return a clean size string given a block_size for a given byte count.
######################################################################
sub sizestring { # sizestring (bytes)

  my $block_size = shift;
  my $bytes = shift;

#  return "" if ( ! $bytes );

  my $byte_factor = $block_sizes{ $block_size };
  my $num_byte_factor = int($bytes / $byte_factor);
  my $rem_bytes = $bytes - $byte_factor * $num_byte_factor;
  my $frac_bytes = int(1000 * ($rem_bytes / $byte_factor));

  if ( $block_size eq "B" ) {
  	# Just put the bytes number with no "B"
  	#return sprintf("%d %s", $num_byte_factor, $block_size);
  	return sprintf("%d", $num_byte_factor);
  } else {
    return sprintf("%d.%03d %s", $num_byte_factor, $frac_bytes, $block_size);
  }

} # End of sizestring()
