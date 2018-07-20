#!/usr/bin/env perl
		# Remove colon prefix if any - sometimes script comes back with one.

use strict;

# Script processes a daily data file of data-extract requests containing rows like:
# ----------------------------------------------------------------------
# Example data file entries:
#       IP           DateTime           Bytes  UserAgent               Error				      Requested URL                              
#       --           --------           -----  ---------               -----				      -------------                              
# 177.20.130.209, 2016-08-23T20:00,         0, SOD/3.2.6,              real-time data polling detected, http://service.iris.edu/fdsnws/dataselect/1/query
#  129.71.50.235, 2016-08-23T20:00,       512, Swarm/2.5.9,            real-time data polling detected, http://service.iris.edu/fdsnws/dataselect/1/query
#  136.167.12.22, 2016-08-23T20:00,         0, IRIS-WS-Library/2.0.12, real-time data polling detected, http://service.iris.edu/fdsnws/dataselect/1/query
#  81.149.172.17, 2016-08-23T20:00,         0, ObsPy,                  real-time data polling detected, http://service.iris.edu/fdsnws/dataselect/1/query

# Get data file from args.
my $data_file = $ARGV[0];

if ( ! defined $data_file ) {
	print "\nUSAGE: $0 <extract-data-file>\n\n";
	exit;
}

my $home_dir = "/home/dms/FST_TEST/DATA_EXTRACT_STATS";
my $data_dir = "$home_dir/bin";
my $bin_dir = "$home_dir/bin";

my $hostname_cmd_path = "$bin_dir/gethostname.sh";
my $data_file_path = "$data_dir/$data_file";

if ( ! -e $data_file_path ) {

	die "\nData file $data_file not found at $data_dir\n\n";
}

# Check file format is expected.
my $line = `grep -v "^Host" $data_file_path | head -n1`;
chomp $line;
my @tokens = split /,/, $line;
if ( scalar @tokens != 6 ) {

	die "\nExpected CSV input lines with 6 tokens 'IP, DateTime, Bytes, UserAgent' but got line line [$line]\n\n";
}
 
# Check the file format and get the file data date.
my $report_date = "";
if ( $data_file =~ /(.*)_(.*)\.txt/ ) {

	# Capture the date from the match.
    my $file_base = $1;
    $report_date = $2;
    # print "\nProcessing report file for $report_date";	

} else {

	die ("File $data_file does not match expected format .*_<date>.txt where <date> like " . "yyyy-mm-dd.txt");
}

# Count the number of requests so we can use them in percents.
my $num_requests = ` cat $data_file_path | grep -v "^Host" | wc -l `;
chomp $num_requests;

# Development mode head command to limit number of rows processed for faster testing.
# my $devel_head_cmd = " | head -n 100000 ";
# my $devel_head_cmd = " | head -n 0 ";
my $devel_head_cmd = "";

if ( $devel_head_cmd ne "" ) {
	print "\nWARNING - devel_head_cmd var = '$devel_head_cmd' so processing file with in a DEVEL mode\n";
}

# Define formats used in column specs.
my $f_comma = "comma";
my $f_pct = "%4.1f";

# Whether to do a banded data table - alternate colors on change in first column value in rows.
my $banded = 0;

# Map from IP to hostname.
my %ip_host_map = ();
my $ip_host_map_ref = \%ip_host_map;

# Pre-process file contents to normalize and check for empty fields:
# - Remove spaces.
# - Replace no IP or no User agent with place holders.my $prep_file_cmd = " cat $data_file_path | grep -v \"^Host\" $devel_head_cmd | sed 's/ //g' | awk -F \",\" '(NF == 6) {print ((\$1 == \"\") ? \"NO-IP\" : \$1),\$2,\$3,((\$4 == \"\") ? \"NO-USER-AGENT\" : \$4),\$5,((\$6 == \"\") ? \"NO_ERROR\" : \$6) }' ";
my $prep_file_cmd = " cat $data_file_path | grep -v \"^Host\" $devel_head_cmd | sed 's/ //g' | awk -F \",\" '(NF == 6) {print ((\$1 == \"\") ? \"NO-IP\" : \$1),\$2,\$3,((\$4 == \"\") ? \"NO-USER-AGENT\" : \$4),((\$5 == \"\") ? \"NO_ERROR\" : \$5),\$6 }' ";
# Record how long the report takes.
my $beg_print = ` date -u +%s `;
chomp $beg_print;

# ----------------------------------------------------------------------
# Start HTML.
 
#       font-family: Arial;
#       font-size: 8pt;

print << "EOI";
<html>
<head>
<title>Daily Data Extract Summary</title>

<style type="text/css">

table.datatable {
        margin-left: 50px;
        color:#333333;
        border-width: 1px;
        border-color: #666666;
        border-collapse: collapse;
        font-size: 9pt;
}
table.datatable th {
        border-width: 1px;
        padding: 3px;
        border-style: solid;
        border-color: #666666;
        background-color: #dedede;
        text-align: center;
}
table.datatable td {
        border-width: 1px;
        padding: 3px;
        border-style: solid;
        border-color: #666666;
        text-align: center;
}

</style>

</head>
<body>

<pre>
EOI

my @results = ();

# -------------------------------------------------

# Print top of page anchor.
print " <A name=\"Top\"></A> ";

# Print main title.
print "<B>Data Extract Summary - $data_file_path:</B> <UL>";


# TODO ADD THE ADDIONTAL FIELDS TO THIS SEDCTION
# Print anchor links to each table.
my @titles = (

	# These anchor titles must match titles of tables.
	"Requests by IP-Address and Host Name" ,
	"No-data Requests by IP-Address" ,
	"No-data Requests by UserAgent" ,
	"No-data Requests by UserAgent/Version" ,
        "Requests by UserAgent" ,
        "Requests by UserAgent/Version" ,
	"Top Errors by IP-Address" ,
        "Errors by UserAgent/Version" ,
        "Top Errors by URL Request (Network Level) and IP-Address" ,
	"Top URL Request (Network Level) by IP-Address and UserAgent/Version" ,
	"Top URL Request (Station Level) by IP-Address" ,
	"Requests-per-Hour" ,
	"Histogram of Requests-per-Hour" ,
	"Max Requests-per-Minute" ,
	"Histogram of Requests-per-Minute" ,
#	"List of UserAgent IPs"
);

print "<TABLE class=\"datatable\" >";
foreach my $title ( @titles ) { 
   	chomp($title);
	print " <TR><TD style=\"text-align:left\" > <A href=\"#$title\">$title</A> </TD></TR> ";
}

print "</TABLE>";

# -------------------------------------------------

print "\n";
print "<B>Total number of Requests = " . &commify($num_requests) ."</B>\n";

# -------------------------------------------------

my $title = "Requests by IP-Address and Host Name";
my $header = "IP-Address [$f_comma]Requests [$f_pct]Percent Hostname&ip_to_hostname,0";

@results = ` $prep_file_cmd | awk '{print \$1}' | awk -F "/" '{print \$1}' | sort | uniq -c | awk '{print \$2,\$1, int((10000.0*\$1)/$num_requests)/100.0}' | sort -nr -k2,2 | awk 'BEGIN { print "$header" }; (NF == 3) { print \$1,\$2,\$3 }' `;

&print_array($title, \@results, $banded);

# -------------------------------------------------
#
#my $title = "Highest No-data Requests by IP-Address";
# NOTE: the &ip_to_hostname column is generated dynamically as a method call - the ',0' means use column 0 as the argument.
#
#my $header = "IP-Address [$f_comma]Requests [$f_comma]No-data [$f_comma]Percent [$f_comma]MBytes Hostname&ip_to_hostname,0";
#my @results = ` $prep_file_cmd | awk '{print \$1,\$3}' | sort | awk 'BEG {prev=""; num=0; numzero=0; bytes=0 }; (\$1 != prev) {bytes = (int(bytes/100000.0))/10.0; print prev,num,numzero,bytes; num=0; numzero=0; bytes=0; prev=\$1 }; (\$1 == prev) { num = num+1; numzero = numzero + ((\$2 == 0) ? 1 : 0); bytes = bytes+\$2}; END { bytes = (int(bytes/100000.0))/10.0; print prev,num,numzero,bytes} ' | sort -nr -k3,3 | awk 'BEGIN { print "$header" }; (NF == 4 && NR < 21) { print \$1,\$2,\$3,int(1000.0*\$3/\$2)/10.0,\$4 }' `; 
#
#&print_array($title, \@results, $banded);
#
# -------------------------------------------------
#  This result is mnot interesting
# my $title = "Top Error leading to No-data Requests by IP-Address";
# my $header = "IP-Address [$f_comma]Error [$f_comma]Requests [$f_comma]No-data [$f_pct]Percent";
# 
# @results = ` $prep_file_cmd | awk '{print \$1\$5,\$3}' | sort | awk ' { num[\$1] += 1; error[\$1]=\$1; if (\$1 in nodata == 0) { nodata[\$1] = 0} }; (\$2 == 0) { nodata[\$1] += 1 }; END { for (n in num) {print  n, error[n], num[n], nodata[n], int(1000*(nodata[n]/num[n]))/10.0 } } ' | sort -nr -k3,3 | awk 'BEGIN { print "$header" }; (NF == 5 ) { print \$1,\$2,\$3,\$4,\$5 }' `;
# 
# &print_array($title, \@results, $banded);

# -------------------------------------------------
my $title = "No-data Requests by IP-Address";
my $header = "IP-Address [$f_comma]Requests [$f_comma]No-data [$f_pct]Percent";

@results = ` $prep_file_cmd | awk '{print \$1,\$3}' | sort | awk ' { num[\$1] += 1; if (\$1 in nodata == 0) { nodata[\$1] = 0} }; (\$2 == 0) { nodata[\$1] += 1 }; END { for (n in num) {print  n, error[n], num[n], nodata[n], int(1000*(nodata[n]/num[n]))/10.0 } } ' | sort -nr -k3,3 | awk 'BEGIN { print "$header" }; (NF == 4 ) { print \$1,\$2,\$3,\$4 }' `;

&print_array($title, \@results, $banded);

# -------------------------------------------------


my $title = "No-data Requests by UserAgent";
my $header = "UserAgent [$f_comma]Requests [$f_comma]No-data [$f_pct]Percent";

my @results = ` $prep_file_cmd | awk -F "/" '{print \$1}' | awk ' { num[\$4] += 1; if (\$4 in nodata == 0) { nodata[\$4] = 0 } }; (\$3 == 0) { nodata[\$4] += 1 }; END { for (n in num) {print n, num[n], nodata[n], int(1000*(nodata[n]/num[n]))/10.0 } } ' | sort -nr -k3,3 | awk 'BEGIN { print "$header" }; (NF == 4) { print \$1,\$2,\$3,\$4 }'   `;

&print_array($title, \@results, $banded);

# -------------------------------------------------

my $title = "No-data Requests by UserAgent/Version";
my $header = "UserAgent/Version [$f_comma]Requests [$f_comma]No-data [$f_pct]Percent";

my @results = ` $prep_file_cmd | awk ' { num[\$4] += 1; if (\$4 in nodata == 0) { nodata[\$4] = 0 } }; (\$3 == 0) { nodata[\$4] += 1 }; END { for (n in num) {print n, num[n], nodata[n], int(1000*(nodata[n]/num[n]))/10.0 } } ' | sort -nr -k3,3 | awk 'BEGIN { print "$header" }; (NF == 4) { print \$1,\$2,\$3,\$4 }'   `;

&print_array($title, \@results, $banded);

# -------------------------------------------------

my $title = "Requests by UserAgent";
my $header = "UserAgent [$f_comma]Requests [$f_pct]Percent";

@results = ` $prep_file_cmd | awk '{print \$4}' | awk -F "/" '{print \$1}' | sort | uniq -c | awk '{print \$2,\$1, int((10000.0*\$1)/$num_requests)/100.0}' | sort -nr -k2,2 | awk 'BEGIN { print "$header" }; (NF == 3) { print \$1,\$2,\$3 }' `;

&print_array($title, \@results, $banded);

# -------------------------------------------------

my $title = "Requests by UserAgent/Version";
my $header = "UserAgent/Version [$f_comma]Requests [$f_pct]Percent";

@results = ` $prep_file_cmd | awk '{print \$4}' | sort | uniq -c | awk '{print \$2,\$1, int((10000.0*\$1)/$num_requests)/100.0}' | sort -nr -k2,2 | awk 'BEGIN { print "$header" }; (NF == 3) { print \$1,\$2,\$3 }' `;

&print_array($title, \@results, $banded);

# -------------------------------------------------

my $title = "Top Errors by IP-Address";
my $header = "IP-Address [$f_comma]Requests [$f_comma]Error";
@results = ` $prep_file_cmd | awk '{print \$1,\$5}' | sort | uniq -c | sort -nr -k1,1 -k2,2 | head -35 | awk 'BEGIN { print "$header" }; (NF == 3) { print \$2,\$1,\$3}'   `;

&print_array($title, \@results, $banded);

# -------------------------------------------------

my $title = "Errors by UserAgent/Version";
my $header = "IP-Address [$f_comma]Requests [$f_comma]Error";
@results = ` $prep_file_cmd | awk '{print \$4,\$5}' | sort | uniq -c | sort -nr -k1,1 -k2,2 | head -35 | awk 'BEGIN { print "$header" }; (NF == 3) { print \$2,\$1,\$3}'   `;

&print_array($title, \@results, $banded);

# -------------------------------------------------

my $title = "Top Errors by URL Request (Network Level) and IP-Address";
my $header = "IP-Address [$f_comma]Requests [$f_comma]Error [$f_comma]URL";
#awk splitting at & splits the url request at the network and station level. 
@results = ` $prep_file_cmd |  awk -F "&" '{print \$1}' | awk '{print \$1,\$5,\$6}' | sort | uniq -c | sort -nr -k1,1 -k2,2 | head -35 |  awk 'BEGIN { print "$header" }; (NF == 4) { print \$2,\$1,\$3,\$4 }' `;

&print_array($title, \@results, $banded); 


# -------------------------------------------------
#$5 = Error 
#$6 = URL
#
#my $title = "URL Request (Network Level) by IP Address";
#my $header = "IP-Address [$f_comma]Requests [$f_comma]URL";
#@results = ` $prep_file_cmd |  awk -F "&" '{print \$1}' | awk '{print \$1,\$6}' | sort | uniq -c | sort -nr -k1,1| head -50 |  awk 'BEGIN { print "$header" }; (NF == 3) { print \$2,\$1,\$3 }' `;
#
#&print_array($title, \@results, $banded);

# Request is the number of times
# -------------------------------------------------

my $title = "Top URL Request (Network Level) by IP-Address and UserAgent/Version";
my $header = "IP-Address [$f_comma]UserAgent/Version [$f_comma]Requests [$f_comma]URL";
@results = ` $prep_file_cmd |  awk -F "&" '{print \$1}' | awk '{print \$1,\$4,\$6}' | sort | uniq -c | sort -nr -k1,1 -k2,2 | head -35 |  awk 'BEGIN { print "$header" }; (NF == 4) { print \$2,\$3,\$1,\$4 }' `;

&print_array($title, \@results, $banded); 

# -------------------------------------------------

my $title = "Top URL Request (Station Level) by IP-Address";
my $header = "IP-Address [$f_comma]Requests [$f_comma]URL";
@results = ` $prep_file_cmd |  awk -F "&" '{print \$1\$2}' | awk '{print \$1,\$6}' | sort | uniq -c | sort -nr -k1,1 -k2,2 | head -35 |  awk 'BEGIN { print "$header" }; (NF == 3) { print \$2,\$1,\$3 }' `;

&print_array($title, \@results, $banded); 

# -------------------------------------------------

my $title = "Requests-per-Hour";
my $header = "Date [$f_comma]Requests";

@results = ` $prep_file_cmd | awk '{print substr(\$2,1,14)}' | sort | uniq -c | sort -k2,2 | awk 'BEGIN { print "$header" }; { print \$2,\$1}' `;

&print_array($title, \@results, $banded);

# -------------------------------------------------

my $title = "Histogram of Requests-per-Hour";
my $header = "[$f_comma]Requests-per-Hour Hours";

@results = ` $prep_file_cmd | awk '{print substr(\$2,1,14)}' | sort | uniq -c | sort -k2,2 | awk '{ print \$2,\$1}' | awk '{ print 5000*int(\$2/5000.0) }' | sort -n | uniq -c | awk 'BEGIN { print "$header" }; {print \$2, \$1}' `;

&print_array($title, \@results, $banded);

# -------------------------------------------------

my $title = "Max Requests-per-Minute";
my $header = "Minute [$f_comma]Requests-per-Minute";

@results = ` $prep_file_cmd | awk '{print substr(\$2,1,17)}' | sort | uniq -c | sort -nr -k1,1 | awk 'BEGIN { print "$header" }; { print \$2,\$1}' | head -n11 `;

&print_array($title, \@results, $banded);

# -------------------------------------------------

my $title = "Histogram of Requests-per-Minute";
my $header = "[$f_comma]Requests-per-Minute [$f_comma]Minutes";

@results = ` $prep_file_cmd | awk '{print substr(\$2,1,17)}' | sort | uniq -c | awk '{ print \$2,\$1}' | sort -n -k2,2 | awk '{print (100*int(\$2/100))}' | uniq -c | awk 'BEGIN { print "$header" }; {print \$2, \$1}' `;

&print_array($title, \@results, $banded);

# -------------------------------------------------

#my $sep = "\n";
#@results = ` $prep_file_cmd | awk '{print \$4,\$1}' | sort -u | awk 'BEG { prev=""; ips=$sep }; ( \$1 == prev ) { ips=(ips $sep ".." \$2 ) }; ( \$1 != prev ) { print ( $sep prev ":" $sep ".." ips $sep" ) ; prev = \$1 ; ips = \$2 }; END { print ( $sep prev ":" $sep ".." ips $sep ) }' `;

#{
	#my $title = "List of UserAgent IPs";
#
	## Print anchor to this table.
	#print " <A name=\"$title\"></A>";
#
	## Print the title.
	#print "\n";
	#print "\n";
	#print "<B>$title</B>: ";
#
	## Print link to top anchor for return.
	#print " <A href=\"#top\">Top</A> ";
#
	#foreach my $result ( @results ) {
		#print "\n$result";
	#}
#}

# -------------------------------------------------
# Footer: 

my $end_print = ` date -u +%s `;
chomp $end_print;

my $report_sec = ( $end_print - $beg_print );

my $num_tables = @titles;
my $sec_per_table = int(10.0 * $report_sec/$num_tables)/10.0;

print "\n";
print "<b>Error Key</b>\n";
print "Real-timedatapollingdetected = \"Real-time data polling detected, please use our streaming service instead: http://ds.iris.edu/ds/nodes/dmc/services/seedlink\"\n";
print "NO_ERROR	             = \" \". No Error Occured. Data was delivered to client.\n";
print "Nodataselected               = \"No data selected\". URL Request was malformed and requested data was not present in the IRIS database.\n";

print "\n";
print "\nPage created <b>" . gmtime() . "</b>\ took $report_sec seconds to generate $num_tables tables ($sec_per_table seconds per table).";

# Print spacer at bottom of page so anchor show the correct spot, else anchor takes us to a page less than bottom of page which isnt always at anchor.
for (my $i = 0; $i <= 40; $i++) {
   print "\n";
}

print << "EOI";
</pre>
</body>
</html>
EOI

exit;

# -------------------------------------------------
sub print_array {

	my $title = shift;

    my $results_ref = shift;
    my @results = @{$results_ref};

    my $banded = shift;

	# Print anchor to this table.
	print " <A name=\"$title\"></A>";

	# Print the title.
	print "\n";
	print "\n";
	print "<B>$title</B>: ";

	# Print link to top anchor for return.
	print " <A href=\"#top\">Top</A> ";

	print "<BR>\n";

    chomp(@results);
	my $num_results = @results; 

	# Print the table.
	if ( $num_results > 1 ) { 

		print "<TABLE class=\"datatable\" >";

		my $row_bg_color1 = "#FFFFFF";
		my $row_bg_color2 = "#DBDBDB";
		my $row_bg_color = $row_bg_color2;
		my $last_col_val = "";
	
		my $rownum = 0;
		my $num_columns = 0;

		# Column spec may indicate a format prefix in [format] preceding the column name.
		my @col_formats;	
	
		# Column spec may indicate a format suffix &functionname(column_number) succeeding the column name.
		my @col_functions;	
	    my $has_col_function = 0;

		my $num_percent_col = -1;

		foreach my $result ( @results ) {

        	my @columns = split ( ' ' , $result );
			chomp @columns;

			# Handle the header.
			if ( $rownum == 0 ) {

				$num_columns = @columns;				

				# Print header columns.
				print "<TR>";

				my $col_num = 0;
				foreach my $column ( @columns ) {

					# Check for column format prefix on column name - format is prefix in square brackets [] on column name.
					if ( $column =~ /\[(.*)\].*/ ) {
			
						# Capture the column format.
						my $col_format = $1;
						$col_formats[$col_num] = $col_format;

						# Remove the format from the column name.
						$column =~ s/^\[.*\]//;

					} else {

						$col_formats[$col_num] = "";
					}
					
					# Check for column function suffix on column name - function is suffix after & on end of column name.
					if ( $column =~ /.*&(.*)/ ) {

						# Capture the column function.
						my $col_function = $1;
						$col_functions[$col_num] = $col_function;
	    				$has_col_function = 1;

						# Remove the function suffix  from the column name.
						$column =~ s/&.*$//;

					} else {

						$col_functions[$col_num] = "";
					}

					# Print the column header.
					print "<TH>$column</TH>";

					$num_percent_col = $col_num if ( $column eq "Percent" );

					$col_num++;
				}
				print "</TR>";

			} else {

				# Print data.

				# Invoke any column functions to supplement the column values.
	    	    if ( $has_col_function ) {

					for ( my $icol = 0; $icol <= $num_columns; $icol++ ) {

						# If column has a column function.
						my $col_function = $col_functions[$icol];

						if ( "$col_function" ne "" ) {

							# Split the function spec "func_name,arg_num"
							my ( $func_name, $func_arg_num ) = split ( ',' , $col_function );

							# Retrieve the argument value by column number.
							my $col_arg = @columns[$func_arg_num];

							# Get reference to the function.
							my $sub = \&{$func_name};

							# Invoke the function on the column arguments.
							# TODO - currently only supports single argument functions.
							my $col_value = $sub->($col_arg); # "CALCULATED";

							# Save the calculated column value onto the columns array.
							$columns[$icol] = $col_value;
						}
					}
				}
				
				my $num_data_columns = @columns;
				
				# Change format based off change in column[0].
        		my $col_val = $columns[0];

        		if ( $col_val ne $last_col_val ) {
                	$row_bg_color = ( $row_bg_color eq $row_bg_color1 ) ? $row_bg_color2 : $row_bg_color1;
                	$last_col_val = $col_val;
        		}

				# Print row columns - set color if banded.
				if ( $banded ) {
        			print "<TR style=\"background-color:$row_bg_color\" >";
				} else {
        			print "<TR>";
				}

				# Print column value.
				my $col_num = 0;
				foreach my $column ( @columns ) {

					# Format column data values here if necessary.
					my $col_format = $col_formats[$col_num];	
			    	my $style = ""; 

					if ( "$col_format" eq "$f_comma" ) {

						$column = &commify($column);
						$style = "style=\"text-align:right\"";

					} elsif ( "$col_format" ne "" ) {

						$column = sprintf("$col_format", $column);
						$style = "style=\"text-align:right\"";

					} else {

						$style = "style=\"text-align:left\"";
					} 

				if ( $col_num == $num_percent_col ) {
					$column = sprintf("%5.1f", $column);
					$column = ( $column > 90.0 ) ? "<FONT COLOR=\"Red\"><B>$column</B></FONT>" :
						  ( $column > 80.0 ) ? "<FONT COLOR=\"DarkOrange\"><B>$column</B></FONT>" :
						  ( $column > 50.0 ) ? "<B>$column</B>" :
						  $column;
				}

        			print "<TD $style >$column</TD>";

					$col_num++;
				}

        		print "</TR>";
			}

			$rownum++;
		}

		print "</TABLE>";

	} else {

		print "NO RESULTS!";
	}
}

# -------------------------------------------------
sub commify {

	my $number = shift;
    my $text = reverse $number;
    $text =~ s/(\d\d\d)(?=\d)(?!\d*\.)/$1,/g;

    return scalar reverse $text;
}

# -------------------------------------------------
sub ip_to_hostname {

	my $ip = shift;

    # my $ip_host_map_ref = shift;
    # my %ip_host_map = %{$ip_host_map_ref};

	if ( not exists $ip_host_map_ref->{$ip} ) {

		my $hostname = ` host $ip | awk -F"pointer " ' { print \$2} ' `;
		#my $hostname = ` $hostname_cmd_path $ip `;
		chomp $hostname;

		# Remove colon prefix if any - sometimes script comes back with one.
		$hostname =~ s/^://;
                substr ($hostname, -1) = "";
		# Remove colon prefix if any - sometimes script comes back with one.
		if($hostname eq "")
		{
		$hostname = "UNKNOWN_IP";
		}

		$ip_host_map_ref->{$ip} = $hostname;
	}

	return $ip_host_map_ref->{$ip};
}

