#! /usr/software/bin/perl

# This tool can be used to create a directory tree of user specified depth
# and width. Th number of files in each of the directories is also user 
# configurable. In addition, the user can specify such things as the percentage
# of the data created that is compressible and dedupable. The user can also
# ask the tool to throttle itself by specifying the MBPS at which the data
# population should occur. User can request modification patterns such as
# the number of files to be created, deleted, grown, shunk, and overwritten
# as a percentage of the number of files in the existing directory tree.
# This tool can also be used to populate a LUN that is mounted on an iSCSI
# client or a single large file can also be modified. For the LUN and single
# file user can specify applicable options like compression and dedupe 
# percentages, percentage of the file or LUN to overwrite, etc. Other
# parameters like MOD_CREATE_PCT, MOD_DELETE_PCT, etc are not applicable
# since those parameters make sense only for directory tree modifications.

sub _usage
{
    return <<'END';
Usage:

    create_tree.pl <options> { <dir-path> | <lun-device-path> | <single-file-path> }

    <options> are specified in the key=value format.

    Possible keys and their default values are:

    BYTES_PER_SEC        - The desired rate at which to write/read data, default is no limit. E.g.
			   values are 300kbps, 20MBPS, etc.
    COMPRESSION_PCT      - Desired percentage savings from compression, default is 0. E.g. values
			   are 30%, 50, etc.
    CONFIG_FILE          - Path to the text file containing these options (one option per line). If
			   in addition to config file the parameters are supplied on the command line, the
			   values from the command line will override the ones from the config file.
    DATASET_TYPE         - Two values are possible: 'small_files' or 'large_files'. For each of these
			   datasets, default values are chosen to model the small and large file environments.
			   Run the command with this option and OPERATION_TYPE set to 'estimate' to figure
			   out what values are used for the different parameters that are available.
    DEDUPE_PCT           - Desired percentage savings from dedupe. Dedupe saving calculated on
			   data remaining after compression, default is 0. E.g. values are 50%, 30, etc.
    DIR_CNT              - Number of sub directories to create in each directory, default is 1.
    DIR_DEPTH            - Depth of the highest level directories, default is 1.
    FILE_CNT             - Number of files to create per directory, default is 1.
    FILE_FILL_PCT        - Percentage of newly created files' size to be filled with data; rest
			   being holes, default is 100%.
    FILE_OR_LUN_PATH     - This parameter is used only when a single file or LUN is being populated.
			   The path from the volume root where the file or LUN being populate/modified
			   exists. This parameter should be specified when MOD_CLONE_PCT option is used.
			   Example value: /LUN1, /Qtree1/LUN2, /Directory/New/File1, etc.
    FILE_SIZE            - Size of files created in each directory, default is 4096. E.g. values
			   are 22k, 20MB, 1g, etc.
    MGMT_IP              - The management IP address of the cluster. Needed when clones need to
			   be created.
    MOD_CREATE_PCT       - Percentage of files to create during each modify pahase, default is 0. E.g.
			   values are 4%, 5, etc.
    MOD_DELETE_PCT       - Percentage of files to delete during each modify pahase, default is 0. E.g.
			   values are 5%, 6, etc.
    MOD_GROW_PCT         - Percentage of files to grow during each modify pahase, default is 0.
    MOD_GROW_BY_PCT      - Percentage of the file's size by which to grow each of the files that
			   are selected for grow operation, default is 0.
    MOD_GROW_BY_SIZE     - Size by which the files are grown each time; default is 0. Example values are
			   22k, 16KB, etc. MOD_GROW_BY_PCT and MOD_GROW_BY_SIZE are mutually exclusive
			   hence only one of them can specified at any time.
    MOD_PUNCHHOLE_PCT    - Percentage of files to punch hole in during each modify phase, default is 0
    MOD_PUNCHHOLE_BY_PCT - Percentage of the file to be filled with holes for each of the files
			   that are selected for hole punching, default is 50.
    MOD_OVERWRITE_PCT    - Percentage of files to overwrite during each modify phase, default is 0.
    MOD_OVERWRITE_BY_PCT - Percentage of the file to be overwritten for each of the files
			   that is selected for overwrite operation, default is 100.
    MOD_SHRINK_PCT       - Percentage of files to shrink during each modify pahase, default is 0.
    MOD_SHRINK_BY_PCT    - Percentage of the file's size by which to shrink each of the files
			   that are selected for shrinking, default is 0.
    MOD_SHRINK_BY_SIZE   - Size by which the files are shrunk each time; default is 0. Example values are
			   22k, 16KB, etc. The options MOD_SHRINK_BY_PCT and MOD_SHRINK_BY_SIZE are
			   mutually exclusive, hence only one of them can specified at any time.
    MOD_CLONE_PCT        - Percentage of files to clone during each modify pahase, default is 0.
			   If this option is used MGMT_IP, VOL_NAME, VSERVER_NAME, USERNAME, and
			   PASSWORD must be provided.
    MODIFY_INTERVAL_SECS - Interval between 2 successive modify cycles, default is back to back.
    NUM_RUNS             - Number of times to execute the modify cycle, default is 0.
    OPERATION_TYPE       - Possible values are populate, modify, read, or estimate. The 'estimate' option
			   provides an estimate of the amount of data that will be modified. The "read"
                           operation type generates only read traffic.
    PASSWORD             - Passowrd to used to connect while sending ZAPI request to the cluster.
    PATH_TO_START_DIR    - The path from the volume root where the data population is occurring. This
			   is an options parameter that should be specified when MOD_CLONE_PCT option
			   is used and when the starting location of the data popuation is not the root.
			   If the starting location of data population is the root of the volume, this
			   parameter is not required. Example value: /Directory/New when the directory
			   tree is being constructed at /vol/<volname>/Directory/New.
    PROGRESS_REPORT      - Seconds between printing progress reports, default is 0 (no reporting).
    READ_PCT             - Percentage of files to read; applies only when OPERATION_TYPE is "read"; 
			   default value is 10%.
    USERNAME             - Username to be used to connect to the cluster while creating clones via ZAPIs.
    VOL_NAME             - Name of the volume being written to.
    VSERVER_NAME         - Name of the Vserver in which the volume exists.

END
}

use strict;
use warnings;

use Fcntl;
use Time::HiRes qw(gettimeofday usleep);
use File::Basename qw(dirname);
use lib dirname(__FILE__);
use POSIX;

# This script requires the NetApp ZAPI SDK libraries.
# If they exist in the callers default PERL5LIB path already, great.
# Otherwise, try looking for the newest internal version in SDK_PATH.
# This internal version can only be used by UNIXes and not windows
# machines.

my $NUM_ENTRIES_PER_DIRBLOCK = 53;
my $ONE_BLOCK = 4096;
my $CG_SIZE = 32768;
my $cannot_clone = 0;
my $os = $^O;
my $index_file_baseline_handle;
my $index_file_incre_handle;
my $MODE;

print "Running on: $os\n";

eval {
    require NaServer;
    require NaElement;
};
if ($@) {
    if ($@ =~ /\ACan\'t locate [\w\/]+\.pm in \@INC/) {
	    if ($os =~ /MSWin/) {
	        # Cannot do any cloning operation; We are not gonna fail due to
	        # this. Later, during data modification, we'll just log a warning
	        # messages everytime we attempt to clone a file.
	        $cannot_clone = 1;
	    } else {
            die $@;
        }
    } else {
	    die $@;
    }
}
import NaServer;
import NaElement;


sub _roundUp
{
    my $num = shift;
    my $base = shift;

    #return $num;
    return (int(($num + $base - 1) / $base) * $base);
}

sub _roundDown
{
    my $num = shift;
    my $base = shift;

    return (int($num / $base) * $base);
}

#
# Print regular progress reports, if mandated.
# This little block is like a mini-module that
# supports configuration of the feature.
#
PROGRESS_REPORT: {

my $last = 0;
my $threshold = 0;  # in seconds: 300 is five minutes, 3600 is one hour
my $all_calls = 0;
my $reporting_calls = 0;

sub _reportPeriodically
{
    return if $threshold == 0; # do no work if disabled
    $all_calls++;
    my $now = time;
    my $elapsed = $now - $last;
    if ($elapsed > $threshold) {
        $reporting_calls++;
        local $| = 1;
        my $dcount = shift;
        my $fcount = shift;
        my $bytes = _getSizeInString(shift, 1024);
        my $work = shift;
	my $stamp = localtime;
        print "$stamp (Finished Dirs: $dcount, Files: $fcount, Data: $bytes); $work\n";
        $last = $now;
    }
}

sub _setReportPeriod
{
    my $interval = shift;
    my $rc;
    if ($interval =~ /^([1-9][0-9]*)s?$/) {
        my $seconds = $1;
        $threshold = $seconds;
        $rc = 0;
    }
    elsif ($interval =~ /^([1-9][0-9]*)m$/) {
        my $minutes = $1;
        $threshold = $minutes * 60;
        $rc = 0;
    }
    else {
        #
        # Disable. Even if the caller didn't check the return,
        # the least harmful is to not have progress reporting
        # (and then it's obvious to a user that something other
        # that what they expected happened, since they were
        # probably expecting the script to actually print some
        # kind of progress.
        #
        $threshold = 0;
        $rc = -1;
    }
    return $rc;
}

} # end PROGRESS_REPORT block

# Get a buffer of the specified size; If compression perecent
# is specified, fill the buffer with that percent of zeros and
# for the rest, fill random data which shouldn't be compressible
sub _getBuf 
{
    my ($dataSetInfoHash_ref) = shift;
    my $compression_pct = shift;
    my $size = shift;

    my $num_bytes_to_fill = $size >= $CG_SIZE ? $CG_SIZE : $size;
    $dataSetInfoHash_ref->{"buf_size"} = $num_bytes_to_fill;

    my $num_wafl_blocks = _roundUp($num_bytes_to_fill, 4096) / 4096;

    if ($compression_pct == 0 || $num_bytes_to_fill <= 4096) {
        my $i = 0;
	while ($i < $num_bytes_to_fill) {
	    # Modify one block randomly so that there can be no deduplication
	    substr($dataSetInfoHash_ref->{"un_compressed_buf"}, $i + int(rand(2047)), 1) = pack( "C", rand(0xff));
	    $i += 2048;
	}
	$dataSetInfoHash_ref->{"num_fill_compressed_bytes"} = 0;
	$dataSetInfoHash_ref->{"num_fill_uncompressed_bytes"} = $num_bytes_to_fill;
	$dataSetInfoHash_ref->{"buf_compressed_bytes"} = 0;
	return; 
    }
	    
    my $num_compress_bytes = int(($num_bytes_to_fill * $compression_pct) / 100);

    my $num_non_compress_bytes = $num_bytes_to_fill - $num_compress_bytes;

    if ($num_non_compress_bytes > 512 && ($num_non_compress_bytes % 4096) == 0) {
	$num_non_compress_bytes -= 512;
	$num_compress_bytes += 512; 
    }

    my $num_uncompressed_blocks = (_roundUp($num_non_compress_bytes, 4096) / 4096);
    $dataSetInfoHash_ref->{"buf_compressed_bytes"} = ($num_wafl_blocks - $num_uncompressed_blocks)*4096;
    
        my $i = 0;
	while ($i < $num_non_compress_bytes) {
	    # Modify one block randomly so that there can be no deduplication
	    substr($dataSetInfoHash_ref->{"un_compressed_buf"}, $i, 1) = pack( "C", rand(0xff));
	    $i += 1024;
	}
    
    $dataSetInfoHash_ref->{"num_fill_compressed_bytes"} = $num_compress_bytes;
    $dataSetInfoHash_ref->{"num_fill_uncompressed_bytes"} = $num_non_compress_bytes;
    return;
}

# Based on how much compression has been achieved so far and
# what the goal is, determine how much compression to do for the
# next buffer of the specified size. Since compression happens
# on block boundaries, we can only play with 13%, 26, 38%, 51%, 
# 76%, and 87%.
sub _calcNextCompressionPct
{
    my $compression_pct = shift;
    my $num_compressed_bytes = shift;
    my $num_total_bytes = shift;
    my $size_to_write = shift;

    my $num_next_total_bytes = $num_total_bytes + $size_to_write;

    my $target_compressed_bytes = int(($num_next_total_bytes * $compression_pct) / 100);   

    my $difference = $target_compressed_bytes - $num_compressed_bytes;

    $difference = $difference < 0 ? 0 : $difference;

    #print "Difference $difference\n"; 

    my $target_pct = ($difference < $size_to_write) ? (($difference/$size_to_write)*100) : 87.5;

    $target_pct = $target_pct >= 25 ? $target_pct : 25;

    return $target_pct;
}

sub _populateFiles
{
    my $dir_path = shift;
    my ($file_handles_ref) = shift;
    my $file_cnt = shift;
    my ($inputHash_ref) = shift;
    my ($dataSetInfoHash_ref) = shift;
    my $cur_file_fill_pct = shift;   
 
    my $size = $dataSetInfoHash_ref->{"write_bytes"};
    my $iter = 0;

    my $cur_file_size = 0;
    my $cur_file_offset = 0;
    my $total_databytes_written = 0;
    my $i = 0;
    
    while ($size > 0) {
	
	#Shantha In Populate function  
	if (defined $MODE && $MODE eq "Random"){
	    # From 1 to 64 Bytes : Randomly pick
	    my @set_sizes = ( 1 .. 32 );
	    my $rand_file_size = $set_sizes[rand(@set_sizes)];
	    $dataSetInfoHash_ref->{"write_bytes"} = $rand_file_size;
	}
	
	
	my $cur_buf_compression_pct = 0;
    my $size_to_write;
	my $seek_bytes = 0;
	my $seeked = 0;
	
	
	

	# The compression group format  within an L1 block can be described
	# as: [8*7*]; i.e. the last CG under an L1 is of size 7 blocks; to 
	# account for that, let us check if we are st such a point now; if so,
	# let us write only 7 blocks instead of 8
	if ((($cur_file_offset + 7*4096) % (255*4096)) == 0) {
	    $size_to_write = $size >= 7*4096 ? 7*4096 : $size;
	} else {
	    $size_to_write = $size >= $CG_SIZE ? $CG_SIZE : $size;
	}

	if ($dataSetInfoHash_ref->{$cur_file_fill_pct} > $inputHash_ref->{"fill_pct"}) {
	    # need to do a seek
	    my $target_bytes_written = int((($cur_file_size + $size_to_write)*$inputHash_ref->{"fill_pct"})/100);
	    my $target_hole_bytes = ($cur_file_size + $size_to_write) - $target_bytes_written;
	    my $difference = $target_hole_bytes - ($cur_file_size - $total_databytes_written);
	    
	    assert($difference >= 0);
	    $seek_bytes = $difference <= $CG_SIZE ? $CG_SIZE : (int($difference / $CG_SIZE))*$CG_SIZE;
	   
	    $seeked = 1;
	    if ($seek_bytes > $size) {
		truncate($file_handles_ref->[$i], $cur_file_size+$size);
	        $seek_bytes = $size;
	    } else {
		my $tmp1 = $cur_file_offset + $seek_bytes;
		my $tmp2 = _roundDown($tmp1, 255*4096);
		my $diff = _roundDown($tmp1 - $tmp2, $CG_SIZE);

		$tmp2 = $tmp2 + $diff;
		$seek_bytes = $tmp2 - $cur_file_offset;
		sysseek($file_handles_ref->[$i], $seek_bytes, SEEK_CUR) 
                or die "Failed to seek by $seek_bytes for file: $dir_path $i: $!";
	    }
	} else {
	    my $use_dedupe_only = 0;

	    if ($dataSetInfoHash_ref->{"cur_compression_pct"} <= $inputHash_ref->{"compression_pct"} &&
		$dataSetInfoHash_ref->{"cur_dedupe_pct"} <= $inputHash_ref->{"dedupe_pct"}) {
		# reuse the existing buf that is both dedupable and compressible
		if ($inputHash_ref->{"compression_pct"} == 0) {
		    # Need a buffer that is dedupable only 
		    $use_dedupe_only = 1;
		    $dataSetInfoHash_ref->{"num_deduped_bytes"} += _roundUp($size_to_write, 4096);
	            $dataSetInfoHash_ref->{"deduped_bufs"}++;
		} elsif ($size_to_write != $dataSetInfoHash_ref->{"buf_size"} ||
		    $inputHash_ref->{"dedupe_pct"} == 0 ||
		    ($inputHash_ref->{"compression_pct"} > 0) &&
		     $dataSetInfoHash_ref->{"buf_compressed_bytes"} == 0) {
		    my $required_compression_pct = _calcNextCompressionPct(
						$inputHash_ref->{"compression_pct"},
						$dataSetInfoHash_ref->{"num_compressed_bytes"},
						$dataSetInfoHash_ref->{"num_total_bytes"},
						$size_to_write);
		    _getBuf($dataSetInfoHash_ref, $required_compression_pct, $size_to_write);
	            $dataSetInfoHash_ref->{"num_compressed_bytes"} += $dataSetInfoHash_ref->{"buf_compressed_bytes"};
	            $dataSetInfoHash_ref->{"new_bufs1"}++;
		} elsif ($inputHash_ref->{"compression_pct"} == 0) {
		    # Need a buffer that is dedupable only 
		    $use_dedupe_only = 1;
		    $dataSetInfoHash_ref->{"num_deduped_bytes"} += _roundUp($size_to_write, 4096);
		    $dataSetInfoHash_ref->{"deduped_bufs"}++;
		} else {
		    # reusing the same buffer, so use the old numbers for dedupe and compress byte counts
		    $dataSetInfoHash_ref->{"num_deduped_bytes"} += _roundUp($size_to_write, 4096);
		    $dataSetInfoHash_ref->{"num_compressed_bytes"} += $dataSetInfoHash_ref->{"buf_compressed_bytes"};
	            $dataSetInfoHash_ref->{"deduped_and_compressed_bufs"}++;
	    	} 
	    } elsif ($dataSetInfoHash_ref->{"cur_compression_pct"} <= $inputHash_ref->{"compression_pct"} &&
		     $dataSetInfoHash_ref->{"cur_dedupe_pct"} >= $inputHash_ref->{"dedupe_pct"}) {
		# getNew buf with compression only
		my $required_compression_pct = _calcNextCompressionPct(
						$inputHash_ref->{"compression_pct"},
						$dataSetInfoHash_ref->{"num_compressed_bytes"},
						$dataSetInfoHash_ref->{"num_total_bytes"},
						$size_to_write);
		_getBuf($dataSetInfoHash_ref, $required_compression_pct, $size_to_write);
	        $dataSetInfoHash_ref->{"num_compressed_bytes"} += $dataSetInfoHash_ref->{"buf_compressed_bytes"};
	        $dataSetInfoHash_ref->{"new_bufs2"}++;
	    } elsif ($dataSetInfoHash_ref->{"cur_compression_pct"} > $inputHash_ref->{"compression_pct"} && 
		$dataSetInfoHash_ref->{"cur_dedupe_pct"} <= $inputHash_ref->{"dedupe_pct"}) {
	        # Need a buffer that is dedupable only 
	        $use_dedupe_only = 1;
	        $dataSetInfoHash_ref->{"num_deduped_bytes"} += _roundUp($size_to_write, 4096);
	        $dataSetInfoHash_ref->{"deduped_bufs"}++;
	    } elsif ($dataSetInfoHash_ref->{"cur_compression_pct"} <= $inputHash_ref->{"compression_pct"} && 
		$dataSetInfoHash_ref->{"cur_dedupe_pct"} > $inputHash_ref->{"dedupe_pct"}) {
		    my $required_compression_pct = _calcNextCompressionPct(
						$inputHash_ref->{"compression_pct"},
						$dataSetInfoHash_ref->{"num_compressed_bytes"},
						$dataSetInfoHash_ref->{"num_total_bytes"},
						$size_to_write);
		    _getBuf($dataSetInfoHash_ref, $required_compression_pct, $size_to_write);
	            $dataSetInfoHash_ref->{"num_compressed_bytes"} += $dataSetInfoHash_ref->{"buf_compressed_bytes"};
	            $dataSetInfoHash_ref->{"new_bufs3"}++;
	    } elsif ($dataSetInfoHash_ref->{"cur_compression_pct"} > $inputHash_ref->{"compression_pct"} && 
		$dataSetInfoHash_ref->{"cur_dedupe_pct"} > $inputHash_ref->{"dedupe_pct"}) {
		# Get a new buffer that is neither compressible nor dedupable
		my $required_compression_pct = 0;
		_getBuf($dataSetInfoHash_ref, $required_compression_pct, $size_to_write);
	        $dataSetInfoHash_ref->{"new_bufs4"}++;
	    } else {
		print "compression $dataSetInfoHash_ref->{\"cur_compression_pct\"} and dedupe ".
		      "$dataSetInfoHash_ref->{\"cur_dedupe_pct\"}\n";
	        assert(0);
	    }
	    $dataSetInfoHash_ref->{"total_bufs"}++;

	    if ($use_dedupe_only) {
		    syswrite($file_handles_ref->[$i], $dataSetInfoHash_ref->{"dedupe_only_buf"},
			     $size_to_write)
			     or die "Write failure in dedupe only buffer for $dir_path $i: $!";
	    } else {
		assert($dataSetInfoHash_ref->{"num_fill_compressed_bytes"} + 
			$dataSetInfoHash_ref->{"num_fill_uncompressed_bytes"} == $size_to_write);    
		if ($dataSetInfoHash_ref->{"num_fill_compressed_bytes"} > 0) {
		    syswrite($file_handles_ref->[$i], $dataSetInfoHash_ref->{"compressed_buf"},
			     $dataSetInfoHash_ref->{"num_fill_compressed_bytes"})
			     or die "Write failure in compressed bytes for $dir_path $i: $!";
		}
 
		if ($dataSetInfoHash_ref->{"num_fill_uncompressed_bytes"} > 0)  {
		    syswrite($file_handles_ref->[$i], $dataSetInfoHash_ref->{"un_compressed_buf"}, 
			     $dataSetInfoHash_ref->{"num_fill_uncompressed_bytes"}) 
			     or die "Write failure in uncompressed bytes for $dir_path $i: $!";
		}
	    }

	    $dataSetInfoHash_ref->{"num_total_bytes"} += _roundUp($size_to_write, 4096);
	    $dataSetInfoHash_ref->{"cur_dedupe_pct"} = int(($dataSetInfoHash_ref->{"num_deduped_bytes"} /
							    $dataSetInfoHash_ref->{"num_total_bytes"}) * 100);
	    $dataSetInfoHash_ref->{"cur_compression_pct"} = int(($dataSetInfoHash_ref->{"num_compressed_bytes"} /
	   						         $dataSetInfoHash_ref->{"num_total_bytes"}) * 100);
	}

	$i++;
	if ($i == $file_cnt) {
            my $bytes_written_or_seeked;
	    if ($seeked) {
		$bytes_written_or_seeked = $seek_bytes;
	    } else {
		$bytes_written_or_seeked = $size_to_write;
	    }
	    $size -= $bytes_written_or_seeked;
	    $cur_file_size += $bytes_written_or_seeked;
	    $cur_file_offset += $bytes_written_or_seeked;
	    $total_databytes_written += ($seeked ? 0 : $size_to_write);
	    $dataSetInfoHash_ref->{$cur_file_fill_pct} = int(($total_databytes_written/$cur_file_size)*100);
            $i = 0;
	}
	$iter++;
	_checkForThrottle($inputHash_ref, $dataSetInfoHash_ref);
    }
}

sub _checkForThrottle
{
    my ($inputHash_ref) = shift;
    my ($dataSetInfoHash_ref) = shift;
    my ($i_sec, $i_usec);
    
    ($i_sec, $i_usec) = gettimeofday();
	
    my $cur_bpmsec;
    my $elapsed_msecs = (($i_sec*1000000 + $i_usec) - 
			 ($dataSetInfoHash_ref->{"s_sec"}*1000000 + 
			  $dataSetInfoHash_ref->{"s_usec"})) / 1000;

    $elapsed_msecs = $elapsed_msecs < 1 ? 1 : $elapsed_msecs;
 
    my $target_bpmsec = $inputHash_ref->{"bps"} / 1000;
    $cur_bpmsec = $dataSetInfoHash_ref->{"num_total_bytes"} / $elapsed_msecs;

    if ($inputHash_ref->{"bps"} > 0) {

	while ($target_bpmsec < $cur_bpmsec) {
	    # Sleep for 2 milliseconds if we are going too fast
	    usleep(2000);

	    ($i_sec, $i_usec) = gettimeofday();
	         
	    $elapsed_msecs = (($i_sec*1000000 + $i_usec) - 
		              ($dataSetInfoHash_ref->{"s_sec"}*1000000 + 
                               $dataSetInfoHash_ref->{"s_usec"})) / 1000;
	    $cur_bpmsec = $dataSetInfoHash_ref->{"num_total_bytes"} / $elapsed_msecs;
	}
    }
}

sub _createFiles 
{
    my $dir_path = shift;
    my $file_start_idx = shift;
    my $file_cnt = shift;
    my ($inputHash_ref) = shift;
    my ($dataSetInfoHash_ref) = shift;

    my $block_device = 0;
  
    if (-b $dir_path || -f $dir_path) {
	$block_device = 1;
    }

    # create the specified file or in the case of block devices just open the file 
    my @file_handles;
    my $i = 0;
    my $j = $file_start_idx;
    $i = 0; 
    while ($i < $file_cnt) {
	if ($block_device) {
	    sysopen ($file_handles[$i], $dir_path, O_WRONLY)
		    or die "Can't open block device $dir_path: $!";
	} else {
        ####Shanthu: Code to add random strings to the files
        my @set_chars = ('a' .. 'z', 'A' .. 'Z', '1' .. '9','#','$','*','@');	
		my @set_numbers = ( 4 .. 8 );
		my $rand_file_length = $set_numbers[rand(@set_numbers)];
		my $str = join '' => map $set_chars[rand @set_chars], 1 .. $set_numbers[rand(@set_numbers)];

		my @exten = ('doc','txt', 'xls', 'pdf', 'ppt', 'xpd', 'mp3', 'jpeg', 'zip', 'tar.gz');
		# my $rand_exten =  $exten[rand(@exten)];
		my $file_name = $str."_file".$j; 		
		my $file_path = $dir_path."/".$file_name;
	    sysopen ($file_handles[$i], $file_path, O_WRONLY | O_TRUNC | O_CREAT)	
		        or die "Can't create $file_path: $!";

		print $index_file_baseline_handle "File_Created:$file_path\n";
    
	    # sysopen ($file_handles[$i], $dir_path."/file".$j, O_WRONLY | O_TRUNC | O_CREAT)
		        # or die "Can't create $dir_path/file$j: $!";
	}
	$i++;
	$j++;
    }

    $inputHash_ref->{"fill_pct"} = $inputHash_ref->{"file_fill_pct"};

    # populate the file if nonzero populate size
    _populateFiles($dir_path, \@file_handles, $file_cnt, 
		   $inputHash_ref, $dataSetInfoHash_ref, "cur_create_fill_pct");

    if ($dataSetInfoHash_ref->{"num_total_bytes"}) {
	my $actual_d_percent = int(($dataSetInfoHash_ref->{"num_deduped_bytes"} / 
				$dataSetInfoHash_ref->{"num_total_bytes"}) * 100);
	my $actual_c_percent = int(($dataSetInfoHash_ref->{"num_compressed_bytes"} / 
				$dataSetInfoHash_ref->{"num_total_bytes"}) * 100);
	my $comp_mbs = $dataSetInfoHash_ref->{"num_compressed_bytes"} / (1024*1024);
    }

    # close the file
    for ($i = 0; $i < $file_cnt; $i++) {
        close $file_handles[$i];
    }
}

sub _createClone
{
    my $zapi_conn = shift;
    my ($inputHash_ref) = shift;
    my ($dataSetInfoHash_ref) = shift;
    my $src_file_path = shift;
    my $dst_file_name = shift;
    my $ret = 0;
    my ($s_path, $d_path);
    my ($s_secs, $s_usecs, $e_secs, $e_usecs);

    if ($cannot_clone) {
	print "Cannot create clones because path to NetApp ZAPI libraries are not provided\n";
	return 1;
    }
 
    ($s_secs, $s_usecs) = gettimeofday();
    if ($inputHash_ref->{"block_or_single_file"} == 0) {
	my $dir_path = $inputHash_ref->{"dir_path"};
	if ($src_file_path =~ /\Q$dir_path\E(\S+)$/) {
	    $s_path = $1;
	    if (defined($inputHash_ref->{"path_to_start_dir"})) {
		$s_path = $inputHash_ref->{"path_to_start_dir"}.$s_path;
	    }
	
	    if ($s_path =~ /(\S+)?\/(\S+)$/) {
		$d_path = defined($1) ? $1 : "/";
		if ($d_path =~ /(\S+)?\/$/) {
		    chop($d_path);
	        }
		$d_path = $d_path."/$dst_file_name";
	    } else {
		print "Failed to get source dir path while cloning: $s_path\n";
		return 1;
	    }
	} else {
	    print "Failed to parse dir path while cloning: $src_file_path dir_path: $inputHash_ref->{\"dir_path\"}\n";
	    return 1;
        }
    } else {
	# If we are dealing with a single file or a LUN, the caller is supposed to
	# provide the full path. So we don't need to derive anything here
	$s_path = $src_file_path;
	$d_path = $dst_file_name;
    }

    my @args = ("clone-create",
		"volume", $inputHash_ref->{"vol_name"},
		"source-path", $s_path,
		"destination-path", $d_path);
   
    my $out = $zapi_conn->invoke(@args);
    if (!defined $out) {
	print "failed to invoke clone-create ZAPI\n";
	$ret = 1;
    }

    unless ($out->results_status() eq "passed") {
	print "clone-create zapi failed: " . $out->results_reason . "$s_path\n";
	$ret = 1;
    }
    
    ($e_secs, $e_usecs) = gettimeofday();
    $dataSetInfoHash_ref->{"clone_elapse_usecs"} += (($e_secs*1000000 + $e_usecs) - 
						     ($s_secs*1000000 + $s_usecs)); 
    return $ret;
}

sub _printStats
{
    my ($inputHash_ref) = shift;
    my ($dataSetInfoHash_ref) = shift;
    my $size_str;
 
    $size_str = _getSizeInString($dataSetInfoHash_ref->{"num_total_bytes"}, 1024);
    if ($inputHash_ref->{"operation_type"} =~ /estimate/i) {
        print  "Estimated Data Write = $size_str\n";
	return; 
    } elsif ($inputHash_ref->{"operation_type"} =~ /read/i) {
        print  "Total data read = $size_str\n";
	return; 
    } else {
        print  "Total Data Written = $size_str\n";
    }

    my ($actual_d_percent, $actual_c_percent);

    if ($dataSetInfoHash_ref->{"num_total_bytes"}) {
	$actual_d_percent = int(($dataSetInfoHash_ref->{"num_deduped_bytes"} / 
				    $dataSetInfoHash_ref->{"num_total_bytes"}) * 100);
	$actual_c_percent = int(($dataSetInfoHash_ref->{"num_compressed_bytes"} / 
				    $dataSetInfoHash_ref->{"num_total_bytes"}) * 100);
    } else {
	$actual_c_percent = $actual_d_percent = 0;
    }
	
    my $comp_mbs = _getSizeInString(
			$dataSetInfoHash_ref->{"num_compressed_bytes"}, 1024);
    my $dedupe_mbs = _getSizeInString(
			$dataSetInfoHash_ref->{"num_deduped_bytes"}, 1024);

    print "Logical size of dedupable data written $dedupe_mbs ($actual_d_percent%); ".
    	  "Expected compression savings $comp_mbs ($actual_c_percent%)\n";

    print "New bufs: (1): $dataSetInfoHash_ref->{\"new_bufs1\"}, ".
	  "(2): $dataSetInfoHash_ref->{\"new_bufs2\"}, ".
	  "(3): $dataSetInfoHash_ref->{\"new_bufs3\"}, ".
	  "(4): $dataSetInfoHash_ref->{\"new_bufs4\"}".
	  " Deduped Bufs: $dataSetInfoHash_ref->{\"deduped_bufs\"}, ".
	  "Deduped+Compressed Bufs: $dataSetInfoHash_ref->{\"deduped_and_compressed_bufs\"}, ".
	  "Total Bufs: $dataSetInfoHash_ref->{\"total_bufs\"}\n";
}

sub _getElapsedTime
{
    my $secs = shift;
    my ($days, $hours, $mins);

    $days = $hours = $mins = 0;

    if ($secs >= 24*60*60) {
	$days = int($secs / (24*60*60));
	$secs = $secs % (24*60*60);
    } 

    if ($secs >= 60*60) {
	$hours = int($secs / (60*60));
	$secs = $secs % (60 * 60);	
    } 

    if ($secs >= 60) {
	$mins = int($secs / 60);
	$secs = $secs % 60;
    }

    my $str = "";
    if ($days > 0) {
	$str = "$days Days, ";
    }

    if ($hours > 0) {
	$str = $str . "$hours Hours ";
    }

    if ($mins > 0) {
	$str = $str . "$mins Minutes ";
    }

    if ($secs > 0) {
	$str = $str . "$secs Seconds.";
    }
    return $str;
}

sub createTree
{
    my ($inputHash_ref) = shift;
    my ($dataSetInfoHash_ref) = shift;
    
    my $dir_path = $inputHash_ref->{"dir_path"};
    my $dir_cnt = $inputHash_ref->{"dir_cnt"};
    my $dir_depth = $inputHash_ref->{"dir_depth"};
    my $file_cnt = $inputHash_ref->{"file_cnt"};
    my $file_size = $inputHash_ref->{"file_size"};
    my $compression_pct = $inputHash_ref->{"compression_pct"};
    my $dedupe_pct = $inputHash_ref->{"dedupe_pct"};
    my $bps = $inputHash_ref->{"bps"};
    
    my $done = 0;

    my $cur_dir_path = $dir_path;
    my $cur_level = 0;
    my $cur_position = 0;
    my $parent_dir_idx = 0;
    my @dir_level_indexes = (0) x $dir_depth;
    my $total_dir_cnt = 0;
    my $total_files_cnt = 0;
    my $block_device = 0;
  
    if (-b $dir_path || -f $dir_path) {
	$block_device = 1;
    }

    _reportPeriodically(0, 0, 0, "<starting_up>");

    ($dataSetInfoHash_ref->{"s_sec"}, $dataSetInfoHash_ref->{"s_usec"}) = gettimeofday();
    
    my $i = 0; 
    $dataSetInfoHash_ref->{"compressed_buf"} = "";
    $dataSetInfoHash_ref->{"un_compressed_buf"} = "";
    $dataSetInfoHash_ref->{"dedupe_only_buf"} = "";
    while ($i < $CG_SIZE) {
	# To prevent VBN_ZERO from kicking in make sure that the block in not
	# completely zero filled
	if (($i % $ONE_BLOCK) != 0) {
            $dataSetInfoHash_ref->{"compressed_buf"} = $dataSetInfoHash_ref->{"compressed_buf"} . 
							pack ("C", 0);
        } else {
            $dataSetInfoHash_ref->{"compressed_buf"} = $dataSetInfoHash_ref->{"compressed_buf"} . 
							pack ("C", rand(0xff));
	}
	$dataSetInfoHash_ref->{"un_compressed_buf"} = $dataSetInfoHash_ref->{"un_compressed_buf"} . 
							pack ("C", rand(0xff));
        $dataSetInfoHash_ref->{"dedupe_only_buf"} = $dataSetInfoHash_ref->{"dedupe_only_buf"} . 
							pack ("C", rand(0xff));
        $i++;
    }
   
    $dataSetInfoHash_ref->{"num_total_bytes"} = 0;
    $dataSetInfoHash_ref->{"num_compressed_bytes"} = 0;
    $dataSetInfoHash_ref->{"num_deduped_bytes"} = 0;
    $dataSetInfoHash_ref->{"cur_create_fill_pct"} = 0;
    $dataSetInfoHash_ref->{"cur_compression_pct"} = 0;
    $dataSetInfoHash_ref->{"cur_dedupe_pct"} = 0;
    $dataSetInfoHash_ref->{"total_bufs"} = 0;
    $dataSetInfoHash_ref->{"deduped_and_compressed_bufs"} = 0;
    $dataSetInfoHash_ref->{"deduped_bufs"} = 0;
    $dataSetInfoHash_ref->{"new_bufs1"} = 0;
    $dataSetInfoHash_ref->{"new_bufs2"} = 0;
    $dataSetInfoHash_ref->{"new_bufs3"} = 0;
    $dataSetInfoHash_ref->{"new_bufs4"} = 0;
    $dataSetInfoHash_ref->{"buf_size"} = 0;
 
    $dataSetInfoHash_ref->{"write_bytes"} = $inputHash_ref->{"file_size"};
    while (!$done) {
	# Create directories first
	if ($cur_level == $dir_depth ||
	    ($dir_level_indexes[$cur_level] + 1) % ($dir_cnt + 1) == 0) {
	    $dir_level_indexes[$cur_level] = 0;
	    $cur_level--;
	    if ($cur_level >= 0) {
		$cur_dir_path = substr($cur_dir_path, 0, rindex($cur_dir_path, "\/"));
	    } else {
	        # We must be done
		last;
	    }
	    next;
	}

	my $report_path = $dir_path;
	if ($block_device == 0) {
	    $dir_level_indexes[$cur_level]++;
	    
		####shanthu: Code to add random strings to the directories
        my @set_chars = ('a' .. 'z', 'A' .. 'Z', '1' .. '9');	
		my @set_numbers = ( 4 .. 8 );
		my $rand_file_length = $set_numbers[rand(@set_numbers)];
		my $str = join '' => map $set_chars[rand @set_chars], 1 .. $set_numbers[rand(@set_numbers)];
		# my $temp_cur_dir_path = $cur_dir_path;
		
		$cur_dir_path = $cur_dir_path."/dir".$dir_level_indexes[$cur_level]."_".$str;

		# my $temp_dir_name = "dir".$dir_level_indexes[$cur_level]."_".$str;
		
		
		print $index_file_baseline_handle "File_Created:$cur_dir_path\n";		
	    # $cur_dir_path = $cur_dir_path."/dir".$dir_level_indexes[$cur_level];

	    if (!mkdir($cur_dir_path)) {
		print "Failed to create dir $cur_dir_path : $!\n";
		exit;
	    }
	    $total_dir_cnt++;
	    $dataSetInfoHash_ref->{"total_dirs_cnt"}++;	
	    $report_path = $cur_dir_path;  
	}
	    
	_reportPeriodically($total_dir_cnt, $total_files_cnt, $dataSetInfoHash_ref->{"num_total_bytes"},
			    "Presently at: $report_path");

	# Create the files next
	$i = 0;
	my $min_dedupe_across = 64;
	while ($i < $file_cnt) {
	    my $create_cnt = ($file_cnt - $i) > $min_dedupe_across ? $min_dedupe_across : ($file_cnt - $i);
	    _createFiles($cur_dir_path, 
			 $i, 
			 $create_cnt, 
			 $inputHash_ref,
			 $dataSetInfoHash_ref,
                         $total_dir_cnt,
                         $total_files_cnt);
	    $i += $create_cnt;
            if ($block_device) {
	        $done = 1;
		next;
	    }
	}
	$total_files_cnt += $file_cnt;
	$dataSetInfoHash_ref->{"total_files_cnt"} += $file_cnt;
	$cur_level++;

    }
    print "Created a total of $total_dir_cnt directories and $total_files_cnt files\n";
    
    my $e_sec;
    my $e_usec;
    ($e_sec, $e_usec) = gettimeofday();
    
    my $elapsed_usecs = (($e_sec*1000000 + $e_usec) - 
			 ($dataSetInfoHash_ref->{"s_sec"}*1000000 + $dataSetInfoHash_ref->{"s_usec"})); 
    my $elapsed_secs = $elapsed_usecs / 1000000;
    my $thruput = _getSizeInString(($dataSetInfoHash_ref->{"num_total_bytes"} / $elapsed_secs), 1000);
    my $time = _getElapsedTime($elapsed_secs);
    print "Elapsed time = $time(total secs = $elapsed_secs); Throughput = $thruput /sec\n";

    _printStats($inputHash_ref, $dataSetInfoHash_ref);
}


sub _getBPS
{
    my $s_sec = shift;
    my $s_usec = shift;
    my $dataSize = shift;
    my ($e_sec, $e_usec);
    ($e_sec, $e_usec) = gettimeofday();
    
    my $elapsed_usecs = (($e_sec*1000000 + $e_usec) - ($s_sec*1000000 + $s_usec));
    my $bpus = ($dataSize / (($elapsed_usecs > 0) ? $elapsed_usecs : 1))*1000000;
    return ($bpus, $elapsed_usecs/1000000);
}

sub _readFile
{
    my $file_path = shift; 
    my ($fh_ref) = shift;
    my $file_size = shift;
    my ($inputHash_ref) = shift;
    my ($dataSetInfoHash_ref) = shift;
    my $readFilePct = shift;
    my $buf;
    my $remaining = $file_size;
    my $read_size = 16*4096;
    my $curFileSize = 0;
    my $curFileReadPct = 0;

    while ($remaining > 0) {
	my $read_size2  = ($read_size > $remaining) ? $remaining : $read_size;
	my ($s_sec, $s_usec);

	if ($curFileSize > 0 ) {
	    $curFileReadPct = (($dataSetInfoHash_ref->{"num_total_bytes"}/$curFileSize)*100);
	}

	if ($curFileReadPct > $readFilePct) {
	    sysseek($fh_ref->[0], $read_size, SEEK_CUR);
	    $curFileSize += $read_size;
	    $remaining -= $read_size;
	    next;
	}

	($s_sec, $s_usec) = gettimeofday();
	my $num_read = sysread($fh_ref->[0], $buf, $read_size2);
	if (!defined($num_read) || $num_read < $read_size2) {
	    $dataSetInfoHash_ref->{"num_file_read_failures"}++;
	    print "failed to read daat from $file_path: $!\n";
	    return;
	}
	my ($bpus, $elapsed_secs) = _getBPS($s_sec, $s_usec, $read_size2);
	if ($bpus < 4096) {
	    $dataSetInfoHash_ref->{"secs_disruption"} += $elapsed_secs;
	    $dataSetInfoHash_ref->{"num_disruptions"}++;
	}

	$curFileSize += $read_size2;	
	$dataSetInfoHash_ref->{"num_total_bytes"} += $read_size2;
	$remaining -= $read_size2;
	_checkForThrottle($inputHash_ref, $dataSetInfoHash_ref);
    }
}

sub _performOp
{
    my $op = shift;
    my $file_path = shift;
    my ($inputHash_ref) = shift;
    my ($dataSetInfoHash_ref) = shift;
    # my $file;
 
    my @file_handles;
    my $dir_path = $file_path;
    if ($inputHash_ref->{"operation_type"} =~ /modify/i) { 
	if ($op eq "create") {
	    my $file = "File.".$dataSetInfoHash_ref->{"mod_iter_start_time"}."_".$dataSetInfoHash_ref->{"num_files_created"};
	    $file_path = $file_path."/".$file;
	    sysopen ($file_handles[0], $file_path, O_WRONLY | O_TRUNC | O_CREAT)
		or die "Can't create $file_path: $!";
	} elsif ($op ne "delete" && $op ne "clone") {
	    sysopen ($file_handles[0], $file_path, O_RDWR)
		     or die "Can't create $file_path: $!";
	}
    } elsif ($inputHash_ref->{"operation_type"} =~ /read/i) {
	my $ret = sysopen ($file_handles[0], $file_path, O_RDONLY);
	if (!$ret) {
	    print "failed to open file $file_path: $!\n";
	    $dataSetInfoHash_ref->{"num_file_read_failures"}++;
	    return;
	} 
    }

	# $file_path=~/(.*)\/([^\/]+)$/;
	# my $file_name = $2;
	# my $temp_dir_path = $1; 
    #print "$op ing on $file_path\n";
 
    if ($op eq "create") {
        $dataSetInfoHash_ref->{"write_bytes"} = $inputHash_ref->{"file_size"};
	$inputHash_ref->{"fill_pct"} = $inputHash_ref->{"file_fill_pct"};
	
	
	## Shanthunew

	# print "Filename $1\n";
	print $index_file_incre_handle "File_Created:$file_path\n";
	if ($inputHash_ref->{"operation_type"} =~ /modify/i) {
	    _populateFiles($file_path, \@file_handles, 1, $inputHash_ref, 
			   $dataSetInfoHash_ref, "cur_create_fill_pct");
	}
	$dataSetInfoHash_ref->{"num_files_created"}++;
    } elsif ($op eq "delete") {
	my $ret = 0;
	$dataSetInfoHash_ref->{"write_bytes"} = 0; 
    ## Shanthunew
	print $index_file_incre_handle "File_Deleted:$file_path\n";

	
	if ($inputHash_ref->{"operation_type"} =~ /modify/i) {
    	    $ret = unlink($file_path);
	    # ignoring file deletion failures for now
	    if (!$ret) {
		$dataSetInfoHash_ref->{"num_file_delete_failures"}++;
	    } else {
		$dataSetInfoHash_ref->{"num_files_deleted"}++;
	    }
	} else {
	    $dataSetInfoHash_ref->{"num_files_deleted"}++;
	}
	return;
    } elsif ($op eq "grow") {
	my $file_size = -s $file_path;

	## Shanthunew
	print $index_file_incre_handle "File_Modified:$file_path\n";	
	
	if ($inputHash_ref->{"mod_grow_by_pct"} != 0) {   
	
	    $dataSetInfoHash_ref->{"write_bytes"} = int(($file_size * $inputHash_ref->{"mod_grow_by_pct"}) / 100);
	    $inputHash_ref->{"fill_pct"} = $inputHash_ref->{"file_fill_pct"};
	} else {
	    $dataSetInfoHash_ref->{"write_bytes"} = $inputHash_ref->{"mod_grow_by_size"};
	    $inputHash_ref->{"fill_pct"} = 100;
	}

	if ($inputHash_ref->{"operation_type"} =~ /modify/i) {
	    my $mod1 = $file_size % (255*4096);
	    my $mod2;
	    my $seek_by;
	    if ($mod1 <= 248*4096) {
	    	$mod2 = $mod1 % $CG_SIZE;
		$seek_by = $CG_SIZE - $mod2;
	    } else {
		my $last_CG_SIZE = $CG_SIZE - 4096;
	    	$mod2 = $mod1 % $last_CG_SIZE;
		$seek_by = $last_CG_SIZE - $mod2;
	    }	    
	    $seek_by += $file_size;
	    sysseek($file_handles[0], $seek_by, SEEK_SET)
                    or die "Seek failure to offset $seek_by in $file_path: $!";
								
	    _populateFiles($file_path, \@file_handles, 1, 
		           $inputHash_ref, $dataSetInfoHash_ref, "cur_grow_fill_pct");
	}
	$dataSetInfoHash_ref->{"num_files_grown"}++;
    } elsif ($op eq "shrink") {
	my $file_size = -s $file_path;
	my $new_size = 0;
	if ($inputHash_ref->{"mod_shrink_by_pct"}) {
	     $new_size = int(($file_size * (100 - $inputHash_ref->{"mod_shrink_by_pct"})) / 100);
	} elsif ($file_size > $inputHash_ref->{"mod_shrink_by_size"}) {
	     $new_size = ($file_size - $inputHash_ref->{"mod_shrink_by_size"});
	}
	$dataSetInfoHash_ref->{"write_bytes"} = 0; 
	if ($inputHash_ref->{"operation_type"} =~ /modify/i) {
	    my $ret = truncate($file_handles[0], $new_size) or
				die "truncate failed $file_path: $!\n";
	}
	$dataSetInfoHash_ref->{"num_files_shrunk"}++;
	$inputHash_ref->{"fill_pct"} = 0;
    } elsif ($op eq "overwrite" or $op eq "punchhole") {
	my $file_size;
	if ($inputHash_ref->{"block_or_single_file"}) {
	    my $file_size1 = $inputHash_ref->{"file_size"};
	    my $file_size2 = -s $file_path;
	    $file_size = $file_size1 > $file_size2 ? $file_size1 : $file_size2;
	} else {
	    $file_size = -s $file_path;
	    if ($file_size == 0) {
		$file_size = $inputHash_ref->{"file_size"};
	    }
	}

	my $fill_pct_type;
	if ($op eq "overwrite") {
            $dataSetInfoHash_ref->{"write_bytes"} = $file_size;
	    $inputHash_ref->{"fill_pct"} = $inputHash_ref->{"mod_overwrite_by_pct"};
	    $dataSetInfoHash_ref->{"num_files_overwritten"}++;
	    $fill_pct_type = "cur_modify_fill_pct";
	} elsif ($op eq "punchhole") {
            $dataSetInfoHash_ref->{"write_bytes"} = $file_size;
	    $inputHash_ref->{"fill_pct"} = 100 - $inputHash_ref->{"mod_punchhole_by_pct"};
	    $dataSetInfoHash_ref->{"num_files_hole_punched"}++;
	    $fill_pct_type = "cur_punchhole_fill_pct";
	    if ($inputHash_ref->{"operation_type"} =~ /modify/i) {
                my $ret = truncate($file_handles[0], 0) or
                               die "truncate failed $file_path: $!\n";
	    }
	}
	if ($inputHash_ref->{"operation_type"} =~ /modify/i) {
            _populateFiles($file_path, \@file_handles, 1, 
			   $inputHash_ref, $dataSetInfoHash_ref, $fill_pct_type);
	}
    } elsif ($op eq "clone") {
	my $ret = 0;
	if ($inputHash_ref->{"operation_type"} =~ /modify/i) {
	    my $zapi_conn = shift;
	    my $dst_file = "File.".$dataSetInfoHash_ref->{"mod_iter_start_time"}.
			   "_clone_".$dataSetInfoHash_ref->{"num_files_cloned"};
	    $ret = _createClone($zapi_conn, $inputHash_ref, $dataSetInfoHash_ref, $file_path, 
				$dst_file);
	}
	if ($ret == 0) {
	    $dataSetInfoHash_ref->{"num_files_cloned"}++;
	} else {
	    $dataSetInfoHash_ref->{"num_clone_create_failures"}++;
	}
	$dataSetInfoHash_ref->{"write_bytes"} = 0; # just for estimate feature
	$inputHash_ref->{"fill_pct"} = 0; # just for estimate feature
    } elsif ($op eq "read") {
	my ($file_size, $read_file_pct);
	if ($inputHash_ref->{"block_or_single_file"}) {
	    my $file_size1 = $inputHash_ref->{"file_size"};
	    my $file_size2 = -s $file_path;
	    $file_size = $file_size1 > $file_size2 ? $file_size1 : $file_size2;
	    $read_file_pct = $inputHash_ref->{"read_pct"};
	} else {
	    $file_size = -s $file_path;
	    if ($file_size == 0) {
		$file_size = $inputHash_ref->{"file_size"};
	    }
	    $read_file_pct = 100;
	}

	$dataSetInfoHash_ref->{"num_files_read"}++;
	_readFile($file_path, \@file_handles, $file_size, $inputHash_ref, $dataSetInfoHash_ref,
		  $read_file_pct);
    }

    if ($inputHash_ref->{"operation_type"} =~ /estimate/i) {
	my $size = int (($dataSetInfoHash_ref->{"write_bytes"}*$inputHash_ref->{"fill_pct"})/100);
	$dataSetInfoHash_ref->{"num_total_bytes"} +=  _roundUp($size, 4096);
    } else {
	if (defined($file_handles[0])) {
	    close $file_handles[0];
	}
    }
}

sub _scanTreeOnce
{
    my ($inputHash_ref) = shift;
    my ($dataSetInfoHash_ref) = shift;

    ($dataSetInfoHash_ref->{"s_sec"}, $dataSetInfoHash_ref->{"s_usec"}) = gettimeofday();

    $dataSetInfoHash_ref->{"num_files_created"} = 0;
    $dataSetInfoHash_ref->{"num_files_deleted"} = 0;
    $dataSetInfoHash_ref->{"num_file_delete_failures"} = 0;
    $dataSetInfoHash_ref->{"num_files_grown"} = 0;
    $dataSetInfoHash_ref->{"num_files_shrunk"} = 0;
    $dataSetInfoHash_ref->{"num_files_overwritten"} = 0;
    $dataSetInfoHash_ref->{"num_files_cloned"} = 0;;
    $dataSetInfoHash_ref->{"num_clone_create_failures"} = 0;;
    $dataSetInfoHash_ref->{"num_files_hole_punched"} = 0;
    
    $dataSetInfoHash_ref->{"num_total_bytes"} = 0;
    $dataSetInfoHash_ref->{"num_compressed_bytes"} = 0;
    $dataSetInfoHash_ref->{"num_deduped_bytes"} = 0;
    $dataSetInfoHash_ref->{"cur_compression_pct"} = 0;
    $dataSetInfoHash_ref->{"cur_dedupe_pct"} = 0;
    $dataSetInfoHash_ref->{"deduped_and_compressed_bufs"} = 0;
    $dataSetInfoHash_ref->{"deduped_bufs"} = 0;
    $dataSetInfoHash_ref->{"total_bufs"} = 0;
    $dataSetInfoHash_ref->{"new_bufs1"} = 0;
    $dataSetInfoHash_ref->{"new_bufs2"} = 0;
    $dataSetInfoHash_ref->{"new_bufs3"} = 0;
    $dataSetInfoHash_ref->{"new_bufs4"} = 0;
    $dataSetInfoHash_ref->{"buf_size"} = 0;
    $dataSetInfoHash_ref->{"clone_elapse_usecs"} = 0;   
    $dataSetInfoHash_ref->{"num_files_read"} = 0;
    $dataSetInfoHash_ref->{"num_file_read_failures"} = 0;
    $dataSetInfoHash_ref->{"secs_disruption"} = 0;
    $dataSetInfoHash_ref->{"num_disruptions"} = 0;
    $dataSetInfoHash_ref->{"cur_create_fill_pct"} = 0;
    $dataSetInfoHash_ref->{"cur_grow_fill_pct"} = 0;
    $dataSetInfoHash_ref->{"cur_modify_fill_pct"} = 0;
    $dataSetInfoHash_ref->{"cur_punchhole_fill_pct"} = 0;
 
    my $zapi_conn;
    if ($inputHash_ref->{"mod_clone_pct"} > 0) {
	$zapi_conn = new NaServer($inputHash_ref->{"mgmt_ip"}, 1, 7);
        $zapi_conn->set_admin_user($inputHash_ref->{"username"},
				$inputHash_ref->{"password"});
        if (!defined($zapi_conn)) {
	    print "Connection to $$inputHash_ref->{\"mgmt_ip\"} as ".
		  "$inputHash_ref->{\"username\"} : $inputHash_ref->{\"password\"} failed.\n";
        }
	$zapi_conn->set_vfiler($inputHash_ref->{"vserver"});
    }

    if (-b $inputHash_ref->{"dir_path"} || -f $inputHash_ref->{"dir_path"}) {
	$inputHash_ref->{"block_or_single_file"} = 1;   
        if ($inputHash_ref->{"operation_type"} =~ /modify/i or
	    $inputHash_ref->{"operation_type"} =~ /estimate/i) {
	    my $i;
	    for ($i = 0; 
		($inputHash_ref->{"operation_type"} =~ /modify/i &&  
		 $i < int($inputHash_ref->{"mod_clone_pct"} / 100)); $i++) {
		my $ret;
		$ret = _createClone($zapi_conn, $inputHash_ref, $dataSetInfoHash_ref,
				$inputHash_ref->{"file_or_lun_path"},
			        $inputHash_ref->{"file_or_lun_path"}."_clone_".$i."_".
				$dataSetInfoHash_ref->{"mod_iter_start_time"});
		if ($ret == 0) {
		    $dataSetInfoHash_ref->{"num_files_cloned"}++;
		} else {
		    $dataSetInfoHash_ref->{"num_clone_create_failures"}++;
		}
	    }
	    _performOp("overwrite", $inputHash_ref->{"dir_path"}, $inputHash_ref,
		       $dataSetInfoHash_ref);
	   
	    if ($inputHash_ref->{"operation_type"} =~ /modify/i) {
	    	my $t = $dataSetInfoHash_ref->{"clone_elapse_usecs"} / 1000000;
	    	print "Cloned $dataSetInfoHash_ref->{\"num_files_cloned\"}; (cummulative clone time: $t secs)";
	    	if ($dataSetInfoHash_ref->{"num_clone_create_failures"} > 0) {
		    print("; (Failures: $dataSetInfoHash_ref->{\"num_clone_create_failures\"})\n");
	    	} else {
		    print("\n");
	    	}
	    }
	} else {
	    _performOp("read", $inputHash_ref->{"dir_path"}, $inputHash_ref, $dataSetInfoHash_ref);
	}
	goto done;
    } else {
	$inputHash_ref->{"block_or_single_file"} = 0;
    }
 
    my $done = 0;

    my $cur_dir_path = $inputHash_ref->{"dir_path"};
    my $cur_level = 0;
    my $cur_position = 0;
    my $parent_dir_idx = 0;
    my @dir_level_file_handles;
    my $total_dirs_scanned = 0;
    my $total_files_scanned = 0;
    my $created_so_far = 0;
    my $deleted_so_far = 0;
    my $grew_so_far = 0;
    my $hole_punched_so_far = 0;
    my $shrunk_so_far = 0;
    my $overwrote_so_far = 0;
    my $cloned_so_far = 0;
    my $read_so_far = 0;

    my $create_pct = $inputHash_ref->{"mod_create_pct"};
    my $clone_pct = $inputHash_ref->{"mod_clone_pct"} + $create_pct;
    my $delete_pct = $inputHash_ref->{"mod_delete_pct"};

    # Since these operations are mutually exclusive to
    # delete, this will help the logic work well with 
    # rand function properly      
    my $grow_pct = $inputHash_ref->{"mod_grow_pct"} + $delete_pct; 
    my $shrink_pct = $inputHash_ref->{"mod_shrink_pct"} + $grow_pct;
    my $overwrite_pct = $inputHash_ref->{"mod_overwrite_pct"} + $shrink_pct;
    my $punchhole_pct = $inputHash_ref->{"mod_punchhole_pct"} + $overwrite_pct;
    my $read_pct = 0;

    if (defined($inputHash_ref->{"read_pct"})) {
	$read_pct = $inputHash_ref->{"read_pct"};
    }

    my (@dir_ents, @files, @idx);

    
    my ($s_sec, $s_usec);
    ($s_sec, $s_usec) = gettimeofday();
    opendir($dir_level_file_handles[$cur_level], $cur_dir_path) or 
	    die "Cannot open $cur_dir_path because $!";
    @files = readdir($dir_level_file_handles[$cur_level]);
    my $read_size = ($#files >= $NUM_ENTRIES_PER_DIRBLOCK) ?
		    (($#files / $NUM_ENTRIES_PER_DIRBLOCK) * $ONE_BLOCK) : $ONE_BLOCK;
    my ($bpus, $elapsed_secs) = _getBPS($s_sec, $s_usec, $read_size);
    if ($bpus < 1024) {
	    $dataSetInfoHash_ref->{"secs_disruption"} += $elapsed_secs;
	    $dataSetInfoHash_ref->{"num_disruptions"}++;
    }
    closedir($dir_level_file_handles[$cur_level]);

    $dir_ents[$cur_level] = [@files];
    $idx[$cur_level] = 0;


    my $random_num_based = 1;
    if (defined($dataSetInfoHash_ref->{"total_files_scanned_last_cycle"}) &&
        $dataSetInfoHash_ref->{"total_files_scanned_last_cycle"} < 1000) {
	# When the number of files in the dataset is less than 1000,
	# the random number based scheme doesn't work very well. So 
	# let us use the count based scheme in that case
	$random_num_based = 0;    
    }

    while (!$done) {
	# Create directories first
	my $go_to_lower_level = 0;	
	my $file;
	  
        _reportPeriodically($total_dirs_scanned, $total_files_scanned, 
			    $dataSetInfoHash_ref->{"num_total_bytes"}, "scanning $cur_dir_path");
	
	while ($idx[$cur_level] <= $#{$dir_ents[$cur_level]}) {
	    $file = $dir_ents[$cur_level][$idx[$cur_level]];
	    $idx[$cur_level]++;
	    # if ($file =~ /dir(\d+)/) {  #### Shanthu: Commented this line
	    if ($file ne "." and $file ne ".." && (-d "$cur_dir_path/$file")) { # this would work on all dataset but won't be efficient
	        $cur_dir_path = $cur_dir_path."/".$file;
		
		opendir($dir_level_file_handles[$cur_level + 1], $cur_dir_path) or 
			die "Cannot open $cur_dir_path because $!";  
	        $cur_level++;
    		@files = readdir($dir_level_file_handles[$cur_level]);
    		closedir($dir_level_file_handles[$cur_level]);
    		$dir_ents[$cur_level] = [@files];
    		$idx[$cur_level] = 0;
		$go_to_lower_level = 1;
		last;
	    }
	    if ($file =~ /file(\d+)/ ||
                ($file =~ /File/ &&
		 $file !~ /File.$dataSetInfoHash_ref->{"mod_iter_start_time"}/)) {
		my $file_path = $cur_dir_path."/".$file;
	
		$total_files_scanned++;

		if ($random_num_based) {

		    my $rand = rand(100);
		    if ($read_pct && $read_pct >= $rand) {
			_performOp("read", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$read_so_far++;
			next;
		    } 

		    if ($inputHash_ref->{"mod_create_pct"} && $create_pct >= $rand) {
			# create new file
			_performOp("create", $cur_dir_path, $inputHash_ref, $dataSetInfoHash_ref);
			$created_so_far++;
		    } elsif ($inputHash_ref->{"mod_clone_pct"} && $clone_pct >= $rand) {
			# clone this file
			_performOp("clone", $file_path, $inputHash_ref, $dataSetInfoHash_ref, 
			      $zapi_conn);
			$cloned_so_far++;
		    }
		    if ($inputHash_ref->{"mod_delete_pct"} && $delete_pct >= $rand) {
			# delete this file
			_performOp("delete", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$deleted_so_far++;
			next;
		    }
	        
		    # Only one of the 3 can happen on a file
		    if ($inputHash_ref->{"mod_grow_pct"} && $grow_pct >= $rand) {
			# grow this file
			_performOp("grow", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$grew_so_far++;
		    } elsif ($inputHash_ref->{"mod_shrink_pct"} && $shrink_pct >= $rand) {
			# shrink this file
			_performOp("shrink", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$shrunk_so_far++;
		    } elsif ($inputHash_ref->{"mod_overwrite_pct"} && $overwrite_pct >= $rand) {
			# overwrite this file
			_performOp("overwrite", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$overwrote_so_far++;
		    } elsif ($inputHash_ref->{"mod_punchhole_pct"} && $punchhole_pct >= $rand) {
			# punchhole in this file
			_performOp("punchhole", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$hole_punched_so_far++;
		    }
		} else {
		   if ($read_pct > 
				(($read_so_far / $total_files_scanned) * 100)) {
			_performOp("read", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$read_so_far++;
			next;
		    }

		   if ($inputHash_ref->{"mod_create_pct"} > 
				(($created_so_far / $total_files_scanned) * 100)) {
			# create new file
			_performOp("create", $cur_dir_path, $inputHash_ref, $dataSetInfoHash_ref);
			$created_so_far++;
		    } 

		    if ($inputHash_ref->{"mod_clone_pct"} > 
				(($cloned_so_far / $total_files_scanned) * 100)) {
			# clone this file
			_performOp("clone", $file_path, $inputHash_ref, $dataSetInfoHash_ref, 
			      $zapi_conn);
			$cloned_so_far++;
		    }
		    if ($inputHash_ref->{"mod_delete_pct"} > 
				(($deleted_so_far / $total_files_scanned) * 100)) {
			# delete this file
			_performOp("delete", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$deleted_so_far++;
			next;
		    }
	        
		    # Only one of the 3 can happen on a file
		    if ($inputHash_ref->{"mod_grow_pct"} > 
				(($grew_so_far / $total_files_scanned) * 100)) {
			# grow this file
			_performOp("grow", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$grew_so_far++;
		    } elsif ($inputHash_ref->{"mod_shrink_pct"} > 
				(($shrunk_so_far / $total_files_scanned) * 100)) {
			# shrink this file
			_performOp("shrink", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$shrunk_so_far++;
		    } elsif ($inputHash_ref->{"mod_overwrite_pct"} > 
				(($overwrote_so_far / $total_files_scanned) * 100)) {
			# overwrite this file
			_performOp("overwrite", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$overwrote_so_far++;
		    } elsif ($inputHash_ref->{"mod_punchhole_pct"} > 
				(($hole_punched_so_far / $total_files_scanned) * 100)) {
			# punchhole in this file
			_performOp("punchhole", $file_path, $inputHash_ref, $dataSetInfoHash_ref);
			$hole_punched_so_far++;
		    }
		}
	    }
	}

	if ($go_to_lower_level) {
	    next;
	}

	# else we must be done with all the files and dirs at this level for this dir
	# so let us go up and try more from the higher level 
	$total_dirs_scanned++;
	if ($cur_level > 0) {
	    $cur_dir_path = substr($cur_dir_path, 0, rindex($cur_dir_path, "\/"));
	    $cur_level--;
	} else {
	    $done = 1;
	}
    }
    
    $dataSetInfoHash_ref->{"total_files_scanned_last_cycle"} = $total_files_scanned;

    my $num_modified_files = $created_so_far + 
			     $cloned_so_far +
			     $deleted_so_far +
			     $grew_so_far +
			     $shrunk_so_far +
			     $overwrote_so_far +
			     $hole_punched_so_far; 

    print "Scanned a total of $total_dirs_scanned directories ".
	  "and $total_files_scanned files\n";
    if ($inputHash_ref->{"operation_type"} =~ /read/) {
	print "Num files read: $dataSetInfoHash_ref->{\"num_files_read\"} ";
	if ($dataSetInfoHash_ref->{"num_file_read_failures"} > 0) {
	    print("; (Failures: $dataSetInfoHash_ref->{\"num_file_read_failures\"})\n");
	} else {
	    print("\n");
	}
	if ($dataSetInfoHash_ref->{"num_disruptions"} > 0) {
	    print "Number of disruptions: $dataSetInfoHash_ref->{\"num_disruptions\"} and ".
		  "total time of disruptions $dataSetInfoHash_ref->{\"secs_disruption\"} secs\n";
	}
	goto done;
    }
    
    print "Total files modified $num_modified_files\n";
    print "Created $dataSetInfoHash_ref->{\"num_files_created\"}\n";
    my $t = $dataSetInfoHash_ref->{"clone_elapse_usecs"} / 1000000;
    print "Cloned $dataSetInfoHash_ref->{\"num_files_cloned\"}; (cummulative clone time: $t secs)";
    if ($dataSetInfoHash_ref->{"num_clone_create_failures"} > 0) {
	print("; (Failures: $dataSetInfoHash_ref->{\"num_clone_create_failures\"})\n");
    } else {
	print("\n");
    }
    print "Deleted $dataSetInfoHash_ref->{\"num_files_deleted\"} ";
    if ($dataSetInfoHash_ref->{"num_file_delete_failures"} > 0) {
	print("; (Failures: $dataSetInfoHash_ref->{\"num_file_delete_failures\"})\n");
    } else {
	print("\n");
    }
    print "Grown $dataSetInfoHash_ref->{\"num_files_grown\"}\n";
    print "Shrunk $dataSetInfoHash_ref->{\"num_files_shrunk\"}\n";
    print "Overwritten $dataSetInfoHash_ref->{\"num_files_overwritten\"}\n";
    print "Hole punched $dataSetInfoHash_ref->{\"num_files_hole_punched\"}\n";

done: 

    my ($e_sec, $e_usec);
    ($e_sec, $e_usec) = gettimeofday();
    
    my $elapsed_usecs = (($e_sec*1000000 + $e_usec) - 
			($dataSetInfoHash_ref->{"s_sec"}*1000000 + $dataSetInfoHash_ref->{"s_usec"})); 
    $elapsed_secs = $elapsed_usecs / 1000000;
    my $mbps = _getSizeInString($dataSetInfoHash_ref->{"num_total_bytes"} / $elapsed_secs, 1000);
    my $time = _getElapsedTime($elapsed_secs);
    print "Elapsed time = $time(total secs = $elapsed_secs); Throughput = $mbps /sec\n";
    _printStats($inputHash_ref, $dataSetInfoHash_ref);
}
   
sub _printOptions
{
    my $inputHash_ref = shift;

    if ($inputHash_ref->{"operation_type"} =~ /read/) {
	print "Read = $inputHash_ref->{\"read_pct\"}%; ".
	      "BPS = $inputHash_ref->{\"bps\"} bps\n";
    } else {
	print "Compression = $inputHash_ref->{\"compression_pct\"}%, ".
	      "Dedupe = $inputHash_ref->{\"dedupe_pct\"}%, ".
	      "BPS = $inputHash_ref->{\"bps\"} bps\n";

	if (!(-b $inputHash_ref->{"dir_path"} || -f $inputHash_ref->{"dir_path"})) {

	    print "Dir Path = $inputHash_ref->{\"dir_path\"}\n";
	    print "Dir Count = $inputHash_ref->{\"dir_cnt\"}, ".
	          "Dir Depth = $inputHash_ref->{\"dir_depth\"}, ".
	          "File Count = $inputHash_ref->{\"file_cnt\"}, ";
	}
	my $size_str = _getSizeInString($inputHash_ref->{"file_size"}, 1024);
	print "File Size = $size_str\n";
	
	if (!(-b $inputHash_ref->{"dir_path"} || -f $inputHash_ref->{"dir_path"})) {
	    print "Create = $inputHash_ref->{\"mod_create_pct\"}% ";
	    print "Delete = $inputHash_ref->{\"mod_delete_pct\"}% ";
	}

	print "Clone = $inputHash_ref->{\"mod_clone_pct\"}%\n";

	if (!(-b $inputHash_ref->{"dir_path"} || -f $inputHash_ref->{"dir_path"})) {
	    print "Grow = $inputHash_ref->{\"mod_grow_pct\"}%, ".
	          "Grow By = $inputHash_ref->{\"mod_grow_by_pct\"}%, ".
	          "Grow By Size= $inputHash_ref->{\"mod_grow_by_size\"}\n";
	    print "Shrink = $inputHash_ref->{\"mod_shrink_pct\"}%, ".
	          "Shrink By = $inputHash_ref->{\"mod_shrink_by_pct\"}% ".
	          "Shrink By Size= $inputHash_ref->{\"mod_shrink_by_size\"}\n";
	    print "Overwrite = $inputHash_ref->{\"mod_overwrite_pct\"}%, ";
	}

	print "Overwrite By = $inputHash_ref->{\"mod_overwrite_by_pct\"}%\n";
	
	if (!(-b $inputHash_ref->{"dir_path"} || -f $inputHash_ref->{"dir_path"})) {
		print "Punch Hole = $inputHash_ref->{\"mod_punchhole_pct\"}%, ".
	              "Punch Hole By = $inputHash_ref->{\"mod_punchhole_by_pct\"}%\n";
	}
    } 
    print "Operation Type = $inputHash_ref->{\"operation_type\"}\n";
    print "Mod Interval = $inputHash_ref->{\"modify_interval\"} ".
	  "Num Runs = $inputHash_ref->{\"num_runs\"}\n";
}

sub scanTree
{
    my ($inputHash_ref) = shift;
    my ($dataSetInfoHash_ref) = shift;

    my $dir_path = $inputHash_ref->{"dir_path"};
    my $num_runs = $inputHash_ref->{"num_runs"};   

    my $i = 0;
    $dataSetInfoHash_ref->{"compressed_buf"} = "";
    $dataSetInfoHash_ref->{"un_compressed_buf"} = "";
    $dataSetInfoHash_ref->{"dedupe_only_buf"} = "";
    while ($i < $CG_SIZE) {
	# To prevent VBN_ZERO from kicking in make sure that the block in not
	# completely zero filled
	if (($i % $ONE_BLOCK) != 0) {
            $dataSetInfoHash_ref->{"compressed_buf"} = $dataSetInfoHash_ref->{"compressed_buf"} . 
							pack ("C", 0);
        } else {
            $dataSetInfoHash_ref->{"compressed_buf"} = $dataSetInfoHash_ref->{"compressed_buf"} . 
							pack ("C", rand(0xff));
	}
        $dataSetInfoHash_ref->{"un_compressed_buf"} = 
		$dataSetInfoHash_ref->{"un_compressed_buf"} . pack ("C", rand(0xff));
        $dataSetInfoHash_ref->{"dedupe_only_buf"} = $dataSetInfoHash_ref->{"dedupe_only_buf"} . 
							pack ("C", rand(0xff));
        $i++;
    }

    $dataSetInfoHash_ref->{"total_files_scanned_last_cycle"} = 0;

    my $infinite_read = (($inputHash_ref->{"operation_type"} =~ /read/) && ($num_runs == 0));
 
    for ($i = 0; (($infinite_read == 1) || ($i < $inputHash_ref->{"num_runs"})); $i++) {
	my ($s_sec, $s_usec, $e_sec, $e_usec, $type);

	$type = ($inputHash_ref->{"operation_type"} =~ /modify/) ? "Modify" : "";
	
	my $stamp = localtime;
	my $iter = $i + 1;
	
	#### shanthu - Index file for Incremental
 	# my $index_file_path = $inputHash_ref->{"dir_path"}."/index_file_incremental_".$iter.".txt";
	my $index_file_path = $inputHash_ref->{"index_file_path"};
	if ( -e $index_file_path ) {
        unlink($index_file_path) or die "$index_file_path: $!"
    }
	open($index_file_incre_handle, '>', $index_file_path);
	
	####shanthu
	# print $index_file_incre_handle "\n************* $stamp: $type iteration $iter starting\n";
	
	print "\n************* $stamp: $type iteration $iter starting\n"
		unless ($inputHash_ref->{"operation_type"} =~ /estimate/i);
	($s_sec, $s_usec) = gettimeofday();
	$dataSetInfoHash_ref->{"mod_iter_start_time"} = time;
	_scanTreeOnce($inputHash_ref, $dataSetInfoHash_ref);
	($e_sec, $e_usec) = gettimeofday();
	
	my $elapsed_usecs = (($e_sec*1000000 + $e_usec) - 
			     ($s_sec*1000000 + $s_usec)); 
	my $elapsed_secs = int($elapsed_usecs / 1000000);

	$stamp = localtime;
	
	####shanthu
	# print $index_file_incre_handle "************** $stamp: $type iteration $iter finished in $elapsed_secs secs\n";
    close($index_file_incre_handle);
	
	print "************** $stamp: $type iteration $iter finished in $elapsed_secs secs\n" 
		unless ($inputHash_ref->{"operation_type"} =~ /estimate/i);
	if ($inputHash_ref->{"operation_type"} !~ /read/ &&
	    $i < ($inputHash_ref->{"num_runs"} - 1) &&
            $elapsed_secs < $inputHash_ref->{"modify_interval"}) {
	    my $sleep_interval = $inputHash_ref->{"modify_interval"} - $elapsed_secs; 
	    print "Sleeping for $sleep_interval secs until next iteration start\n";
	    sleep($sleep_interval);
	}
    }
}


sub _getSize
{
    my $qualifier = shift;

    my $multiplier = 1;
    if ($qualifier =~ /[k|K]/ || $qualifier =~ /KB/i) {   
	$multiplier =  1024;
    } 
	  elsif ($qualifier =~ /R/) { #### Shanthu: Added R
	$MODE = "Random";
	$multiplier =  1;
	}

      elsif ($qualifier =~ /[m|M]/ || $qualifier =~ /MB/i) {
	$multiplier =  1024*1024;
    } elsif ($qualifier =~ /[g|G]/ || $qualifier =~ /GB/i) {
	$multiplier =  1024*1024*1024;
    } elsif ($qualifier ne "") {
        $multiplier = -1;
    }
    return $multiplier;
}

sub _getRightPrecision
{
    my $size = shift;
    return (int(($size * 1000.0) + 0.5) / 1000.0);
}

sub _getSizeInString
{
    my ($total_bytes_estimate, $size, $size_str, $one_kb);
    
    $total_bytes_estimate = shift;
    $one_kb = shift;
    if ($total_bytes_estimate > ($one_kb*$one_kb*$one_kb)) {
	$size = $total_bytes_estimate / ($one_kb*$one_kb*$one_kb);
	$size = _getRightPrecision($size);
	$size_str = "$size GB";
    } elsif ($total_bytes_estimate > ($one_kb*$one_kb)) {
	$size = $total_bytes_estimate / ($one_kb*$one_kb);
	$size = _getRightPrecision($size);
	$size_str = "$size MB";
    } elsif ($total_bytes_estimate > $one_kb) {
	$size = _getRightPrecision($total_bytes_estimate) / $one_kb;
	$size_str = "$size KB";
    } else {
	$size = _getRightPrecision($total_bytes_estimate);
	$size_str = "$total_bytes_estimate Bytes";
    }
    return $size_str;
}

sub _isEmptyDir
{
    my $dir_path = shift;
    if (-d $dir_path) {
	my ($dir_fh, $file);
	opendir($dir_fh, $dir_path) or 
		die "Cannot open $dir_path because $!";
	    
	while ($file = readdir($dir_fh)) {
	    if ($file eq "." || $file eq "..") {
	        next;
	    } else {
		return 0;
	    }
	}
	return 1;
    } 
    return 0;
}  
 
sub _getEstimate
{ 
    my $inputHash_ref = shift;
    my $dataSetInfoHash_ref = shift;

    $inputHash_ref->{"num_runs"} = 1;
    my $dir_path = $inputHash_ref->{"dir_path"};
    my $valid_dir_path = 0;

    # determine if there is a valid directory
    # path to explore.
    if (-d $dir_path) {
	my ($dir_fh, $file);
	opendir($dir_fh, $dir_path) or 
		die "Cannot open $dir_path because $!";
	    
	while ($file = readdir($dir_fh)) {
	    if ($file eq "." || $file eq "..") {
	        next;
	    } else {
		$valid_dir_path = 1;
		last;
	    }
	}
    } elsif (-f $dir_path or -b $dir_path) {
	$valid_dir_path = 1;
    } else {
	$valid_dir_path = 0;
    }

    if ($valid_dir_path == 1) {
	scanTree($inputHash_ref, $dataSetInfoHash_ref);
    } else {
	my ($dir_cnt_estimate, $file_cnt_estimate, $total_bytes_estimate);
	$dir_cnt_estimate = 0;
	my ($created, $deleted, $cloned, $grown, $shrunk, $overwritten, $hole_punched);
	my $i = 1;

	while ($i <= $inputHash_ref->{"dir_depth"}) {
	    $dir_cnt_estimate += POSIX::pow($inputHash_ref->{"dir_cnt"}, $i);
	    $i++;
	}
	$file_cnt_estimate = $dir_cnt_estimate * $inputHash_ref->{"file_cnt"};
	$total_bytes_estimate = int(($file_cnt_estimate *
				     $inputHash_ref->{"file_size"} *
				     $inputHash_ref->{"file_fill_pct"})/100);
	    
	my $size_str = _getSizeInString($total_bytes_estimate, 1024);
	print "Populate estimates: File Count=$file_cnt_estimate, ".
	      "Dir Count=$dir_cnt_estimate, Dataset Size=$size_str\n";
 
	$created = int(($file_cnt_estimate * $inputHash_ref->{"mod_create_pct"}) / 100);
	$deleted = int(($file_cnt_estimate * $inputHash_ref->{"mod_delete_pct"}) / 100);
	$cloned = int(($file_cnt_estimate * $inputHash_ref->{"mod_clone_pct"}) / 100);
	$grown = int(($file_cnt_estimate * $inputHash_ref->{"mod_grow_pct"}) / 100);
	$shrunk = int(($file_cnt_estimate * $inputHash_ref->{"mod_shrink_pct"}) / 100);
	$overwritten = int(($file_cnt_estimate * $inputHash_ref->{"mod_overwrite_pct"}) / 100);
	$hole_punched = int(($file_cnt_estimate * $inputHash_ref->{"mod_punchhole_pct"}) / 100);

	$total_bytes_estimate = 
		int(($created * $inputHash_ref->{"file_size"} * $inputHash_ref->{"file_fill_pct"})/100) + 
		($grown * $inputHash_ref->{"mod_grow_by_size"}) +
		int(($grown * $inputHash_ref->{"mod_grow_by_pct"} * $inputHash_ref->{"file_fill_pct"})/100) +
		int(($overwritten * $inputHash_ref->{"file_size"} * $inputHash_ref->{"mod_overwrite_by_pct"})/100) +
		int(($hole_punched * $inputHash_ref->{"file_size"} * (100 - $inputHash_ref->{"mod_punchhole_by_pct"}))/100);
	    
	$size_str = _getSizeInString($total_bytes_estimate, 1024);
	print "Modify estimates: Created=$created, Deleted=$deleted, Cloned=$cloned, ".
	      "Grown=$grown, Shrunk=$shrunk, Overwritten=$overwritten, Hole Punches $hole_punched ".
	      "Total Data write=$size_str\n";
    }
}

sub _parsePctInput
{
    my $input = shift;
    my $key = shift;
    my $out;

    if ($input =~ /$key=([0-9]*\.)?([0-9]+)(\%*)$/) {
	$out = defined($1) ? $1.$2 : $2;
    }
    return $out;
}


sub _parseInputArgs
{
    my $inputArray_ref = shift;
    my $num_params = shift;
    my $inputHash_ref = shift;

    my $i = 0;
    while ($i < $num_params) {
	if ($inputArray_ref->[$i] =~ /^#/)  {
	    #print "Ignoring $inputArray_ref->[$i]";
	} elsif ($inputArray_ref->[$i] =~ /CONFIG_FILE=(\S+)/) {
	    # just ignore the CONFIG_FILE param here
	} elsif ($inputArray_ref->[$i] =~ /DATASET_TYPE=(\S+)/) {
	    # just ignore DATASET_TYPE param as it is already taken care of
	} elsif ($inputArray_ref->[$i] =~ /DIR_CNT=(\d+)$/)  {
	    $inputHash_ref->{"dir_cnt"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /DIR_DEPTH=(\d+)$/)  {
	    $inputHash_ref->{"dir_depth"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /FILE_CNT=(\d+)$/)  {
	    $inputHash_ref->{"file_cnt"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /FILE_SIZE=(\d+)$/ ||   #### Shanthu: Added "R" in the below regex
	         $inputArray_ref->[$i] =~ /FILE_SIZE=(\d+)([k|K|m|M|g|G|R|bytes])([b|B]*)$/)  {
	    my $file_size = $1;
	    if ($inputArray_ref->[$i] =~ /(\d+)$/) {
	    } elsif ($inputArray_ref->[$i] =~ /(\d+)(\w*)$/) {
		$file_size *= _getSize($2);
	    }
		else {
		print "Unknown value,\"$2\", for option FILE_SIZE\n";
		exit;
	    }
	    $inputHash_ref->{"file_size"} = $file_size;
	} elsif ($inputArray_ref->[$i] =~ /FILE_FILL_PCT/)  {
	    $inputHash_ref->{"file_fill_pct"} = _parsePctInput($inputArray_ref->[$i],
								 "FILE_FILL_PCT");;
	} elsif ($inputArray_ref->[$i] =~ /COMPRESSION_PCT/)  {
	    $inputHash_ref->{"compression_pct"} = _parsePctInput($inputArray_ref->[$i],
								   "COMPRESSION_PCT");
	} elsif ($inputArray_ref->[$i] =~ /DEDUPE_PCT/)  {
	    $inputHash_ref->{"dedupe_pct"} = _parsePctInput($inputArray_ref->[$i],
							      "DEDUPE_PCT");
	} elsif ($inputArray_ref->[$i] =~ /BYTES_PER_SEC=([0-9]*\.)?([0-9]+)/)  {
	    my $bps = defined($1) ? $1.$2 : $2;
	    if ($inputArray_ref->[$i] =~ /(\d+)(\w*)$/) {
		my $multiplier = 1;
		if ($2 =~ /kBps/ || $2 =~ /KBps/ || $2 =~ /KBPS/ || $2 =~ /kbps/) {
		    $multiplier =  1000;
		} elsif ($2 =~ /mBps/ || $2 =~ /KBps/ || $2 =~ /MBPS/ || $2 =~ /mbps/) {
		    $multiplier =  1000*1000;
		} elsif ($2 =~ /gBps/ || $2 =~ /GBps/ || $2 =~ /GBPS/ || $2 =~ /gbps/) {
		    $multiplier =  1000*1000*1000;
        	} elsif ($2 ne "") {
		    print "Unknown value,\"$2\", for option BYTES_PER_SEC\n";
		    exit;
		}
        	$bps *= $multiplier;
	    } else {
		print "Unknown option, \"$inputArray_ref->\[$i\]\"\n";
		exit;
	    }
	    $inputHash_ref->{"bps"} = $bps;
	} elsif ($inputArray_ref->[$i] =~ /MOD_CLONE_PCT/)  {
	    $inputHash_ref->{"mod_clone_pct"} = _parsePctInput($inputArray_ref->[$i],
								  "MOD_CLONE_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MOD_CREATE_PCT/)  {
	    $inputHash_ref->{"mod_create_pct"} = _parsePctInput($inputArray_ref->[$i],
								  "MOD_CREATE_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MOD_DELETE_PCT/)  {
	    $inputHash_ref->{"mod_delete_pct"} = _parsePctInput($inputArray_ref->[$i],
								  "MOD_DELETE_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MOD_GROW_PCT/)  {
	    $inputHash_ref->{"mod_grow_pct"} = _parsePctInput($inputArray_ref->[$i],
								  "MOD_GROW_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MOD_GROW_BY_PCT/)  {
	    $inputHash_ref->{"mod_grow_by_pct"} = _parsePctInput($inputArray_ref->[$i],
								   "MOD_GROW_BY_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MOD_GROW_BY_SIZE=(\d+)$/ ||
	         $inputArray_ref->[$i] =~ /MOD_GROW_BY_SIZE=(\d+)([k|K|m|M|g|G])([b|B]*)$/)  {  ##Shanthu: Added Bytes
	    my $grow_by_size = $1;
	    if ($inputArray_ref->[$i] =~ /(\d+)$/)  {
	    } elsif ($inputArray_ref->[$i] =~ /(\d+)(\w*)$/) {
		$grow_by_size *= _getSize($2);
	    } else {
		print "Unknown value,\"$2\", for option MOD_GROW_BY_SIZE\n";
		exit;
	    }
	    $inputHash_ref->{"mod_grow_by_size"} = $grow_by_size;
		print "Grow by size $grow_by_size"; 
	} elsif ($inputArray_ref->[$i] =~ /MOD_SHRINK_BY_SIZE=(\d+)$/ ||
	         $inputArray_ref->[$i] =~ /MOD_SHRINK_BY_SIZE=(\d+)([k|K|m|M|g|G])([b|B]*)$/)  {
	    my $shrink_by_size = $1;
	    if ($inputArray_ref->[$i] =~ /(\d+)$/)  {
	    } elsif ($inputArray_ref->[$i] =~ /(\d+)(\w*)$/) {
		$shrink_by_size *= _getSize($2);
	    } else {
		print "Unknown value,\"$2\", for option MOD_GROW_BY_SIZE\n";
		exit;
	    }
	    $inputHash_ref->{"mod_shrink_by_size"} = $shrink_by_size;
	} elsif ($inputArray_ref->[$i] =~ /MOD_OVERWRITE_PCT/)  {
	    $inputHash_ref->{"mod_overwrite_pct"} = _parsePctInput($inputArray_ref->[$i],
								     "MOD_OVERWRITE_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MOD_OVERWRITE_BY_PCT/)  {
	    $inputHash_ref->{"mod_overwrite_by_pct"} = _parsePctInput($inputArray_ref->[$i],
									"MOD_OVERWRITE_BY_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MOD_PUNCHHOLE_PCT/)  {
	    $inputHash_ref->{"mod_punchhole_pct"} = _parsePctInput($inputArray_ref->[$i],
								     "MOD_PUNCHHOLE_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MOD_PUNCHHOLE_BY_PCT/)  {
	    $inputHash_ref->{"mod_punchhole_by_pct"} = _parsePctInput($inputArray_ref->[$i],
									"MOD_PUNCHHOLE_BY_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MOD_SHRINK_PCT/)  {
	    $inputHash_ref->{"mod_shrink_pct"} = _parsePctInput($inputArray_ref->[$i],
								  "MOD_SHRINK_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MOD_SHRINK_BY_PCT/)  {
	    $inputHash_ref->{"mod_shrink_by_pct"} = _parsePctInput($inputArray_ref->[$i],
								     "MOD_SHRINK_BY_PCT");
	} elsif ($inputArray_ref->[$i] =~ /READ_PCT/)  {
	    $inputHash_ref->{"read_pct"} = _parsePctInput($inputArray_ref->[$i],
								  "READ_PCT");
	} elsif ($inputArray_ref->[$i] =~ /MODIFY_INTERVAL_SECS=(\d+)$/)  {
	    $inputHash_ref->{"modify_interval"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /NUM_RUNS=(\d+)$/)  {
	    $inputHash_ref->{"num_runs"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /PROGRESS_REPORT=(\d+[ms]?)$/)  {
	    my $report_interval = $1;
	    if (_setReportPeriod($report_interval)) {
		print "Unknown PROGRESS_REPORT value \"$report_interval\"\n";
		print "Value should be in seconds (e.g., 15s) or minutes (e.g., 5m)\n";
		exit;
	    }
	} elsif ($inputArray_ref->[$i] =~ /MODIFY_ONLY=(\w+)$/) {
	    if ($1 =~ /true/i) {
		$inputHash_ref->{"operation_type"} = "modify";
	    } elsif ($1 =~ /false/i) {
		$inputHash_ref->{"operation_type"} = "populate,modify";
	    } else {
	        print "Unknown value, \"$1\" for option MODIFY_ONLY\n";
		exit;
	    }
	} elsif ($inputArray_ref->[$i] =~ /VOL_NAME=(\S+)$/) {
	    $inputHash_ref->{"vol_name"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /VSERVER_NAME=(\S+)$/) {
	    $inputHash_ref->{"vserver"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /MGMT_IP=(\S+)$/) {
	    $inputHash_ref->{"mgmt_ip"} = $1;
	}
	  elsif ($inputArray_ref->[$i] =~ /ITERATION=(\S+)$/) {
	    $inputHash_ref->{"incremental_iteration"} = $1;	
	} elsif ($inputArray_ref->[$i] =~ /USERNAME=(\S+)$/) {
	    $inputHash_ref->{"username"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /PASSWORD=(\S+)$/) {
	    $inputHash_ref->{"password"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /PATH_TO_START_DIR=(\S+)$/) {
	    $inputHash_ref->{"path_to_start_dir"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /FILE_OR_LUN_PATH=(\S+)$/) {
	    $inputHash_ref->{"file_or_lun_path"} = $1;
	} elsif ($inputArray_ref->[$i] =~ /OPERATION_TYPE=(\S+)/) {
	    my $op_type = $1;
	    print "Op type is $op_type\n";
	    if ($op_type eq "populate" or
		$op_type eq "modify" or
		$op_type eq "populate,modify" or
		$op_type eq "read" or
		$op_type eq "estimate") {
		$inputHash_ref->{"operation_type"} = $op_type;
	    } else {
	        print "Unknown value, \"$1\" for option OPERATION_TYPE\n";
		exit;
	    }
	} else {
	    print "At line 1990 Unknown option \"".$inputArray_ref->[$i]."\"\n";
	    exit;
	}
	$i++;
    }
}

sub _initParams
{
    my ($inputHash_ref) = shift;

    $inputHash_ref->{"dir_cnt"} = 1;
    $inputHash_ref->{"dir_depth"} = 1;
    $inputHash_ref->{"file_cnt"} = 1;
    $inputHash_ref->{"file_size"} = 4096;
    $inputHash_ref->{"file_fill_pct"} = 100;
    $inputHash_ref->{"bps"} = -1 ;
    $inputHash_ref->{"compression_pct"} = 0;
    $inputHash_ref->{"dedupe_pct"} = 0;
    $inputHash_ref->{"mod_create_pct"} = 0;
    $inputHash_ref->{"mod_delete_pct"} = 0;
    $inputHash_ref->{"mod_grow_pct"} = 0;
    $inputHash_ref->{"mod_clone_pct"} = 0;
    $inputHash_ref->{"mod_shrink_pct"} = 0;
    $inputHash_ref->{"mod_overwrite_pct"} = 0;
    $inputHash_ref->{"mod_punchhole_pct"} = 0;
    $inputHash_ref->{"read_pct"} = 0;
    $inputHash_ref->{"modify_interval"} = 0;
    $inputHash_ref->{"num_runs"} = 0;
    $inputHash_ref->{"mod_grow_by_pct"} = 0;
    $inputHash_ref->{"mod_grow_by_size"} = 0;
    $inputHash_ref->{"mod_overwrite_by_pct"} = 100;
    $inputHash_ref->{"mod_shrink_by_pct"} = 0;
    $inputHash_ref->{"mod_shrink_by_size"} = 0;
    $inputHash_ref->{"mod_punchhole_by_pct"} = 20;
}

sub _initDatasetType
{
    my $inputHash_ref = shift;
    my $dataSetType = shift;

    $inputHash_ref->{"bps"} = -1 ;
    $inputHash_ref->{"file_fill_pct"} = 100;
    $inputHash_ref->{"compression_pct"} = 30;
    $inputHash_ref->{"dedupe_pct"} = 50;
    $inputHash_ref->{"modify_interval"} = 3600;
    $inputHash_ref->{"num_runs"} = 1;
    
    if ($dataSetType =~ /small_files/) {
	$inputHash_ref->{"dir_cnt"} = 5;
	$inputHash_ref->{"dir_depth"} = 5;
	$inputHash_ref->{"file_cnt"} = 51;
	$inputHash_ref->{"file_size"} = 22*1024;
	$inputHash_ref->{"mod_create_pct"} = 6;
	$inputHash_ref->{"mod_delete_pct"} = 8;
	$inputHash_ref->{"mod_clone_pct"} = 2;
	$inputHash_ref->{"mod_grow_pct"} = 1;
	$inputHash_ref->{"mod_grow_by_pct"} = 0;
	$inputHash_ref->{"mod_grow_by_size"} = 10*1024;
	$inputHash_ref->{"mod_overwrite_pct"} = 1;
	$inputHash_ref->{"mod_overwrite_by_pct"} = 100;
	$inputHash_ref->{"mod_punchhole_pct"} = 1;
	$inputHash_ref->{"mod_punchhole_by_pct"} = 20;
	$inputHash_ref->{"mod_shrink_pct"} = 1;
	$inputHash_ref->{"mod_shrink_by_pct"} = 0;
	$inputHash_ref->{"mod_shrink_by_size"} = 10*1024;
    } elsif ($dataSetType =~ /large_files/) {
	$inputHash_ref->{"dir_cnt"} = 1;
	$inputHash_ref->{"dir_depth"} = 1;
	$inputHash_ref->{"file_cnt"} = 400;
	$inputHash_ref->{"file_size"} = 10*1024*1024;
	$inputHash_ref->{"mod_create_pct"} = 2.5;
	$inputHash_ref->{"mod_delete_pct"} = 5;
	$inputHash_ref->{"mod_clone_pct"} = 2.5;
	$inputHash_ref->{"mod_grow_pct"} = 2.5;
	$inputHash_ref->{"mod_grow_by_pct"} = 0;
	$inputHash_ref->{"mod_grow_by_size"} = 1024*1024;
	$inputHash_ref->{"mod_overwrite_pct"} = 10;
	$inputHash_ref->{"mod_overwrite_by_pct"} = 30;
	$inputHash_ref->{"mod_punchhole_pct"} = 5;
	$inputHash_ref->{"mod_punchhole_by_pct"} = 20;
	$inputHash_ref->{"mod_shrink_pct"} = 2.5;
	$inputHash_ref->{"mod_shrink_by_pct"} = 0;
	$inputHash_ref->{"mod_shrink_by_size"} = 1024*1024;
    } else {
	print "Unknown value for DATASET_TYPE\n";   
    }
}

sub _getUserInput
{
    my ($inputArray_ref) = shift;
    my $num_params = shift;
    my ($inputHash_ref) = shift;
    my ($config_file, @lines, $config_file_processed);

    _initParams($inputHash_ref);

    if ((-d $inputArray_ref->[$num_params - 1]) ||
	(-b $inputArray_ref->[$num_params - 1]) ||
	(-f $inputArray_ref->[$num_params - 1])) {
	$inputHash_ref->{"dir_path"} = $inputArray_ref->[$num_params - 1] . "/root_dir"; ## Shanthu : Adding root_dir
	
	$num_params = $num_params - 1;
    } else {
	$inputHash_ref->{"dir_path"} = "";
    }
    
    my $i = 0;
    # first parse the input from the config file
    while ($i < $num_params) {
	if ($inputArray_ref->[$i] =~ /CONFIG_FILE=(\S+)/)  {
    	    open FILE, "<$1" or die "Can't open $config_file because $!\n";
	    @lines = <FILE>;
	    close FILE;
	    _parseInputArgs(\@lines, $#lines+1, $inputHash_ref);
	    last;
	} elsif ($inputArray_ref->[$i] =~ /DATASET_TYPE=(\S+)/) {
	    _initDatasetType($inputHash_ref, $1);
	    last;
	}
	$i++;
    }

    # Now parse the override arguments
    _parseInputArgs($inputArray_ref, $num_params, $inputHash_ref);

    if ($inputHash_ref->{"mod_grow_by_size"} != 0 &&
	$inputHash_ref->{"mod_grow_by_pct"} != 0) {
	print "Can specify only one of MOD_GROW_BY_SIZE or MOD_GROW_BY_PCT; Not Both\n";
	print _usage();
	exit;
    } 

    if ($inputHash_ref->{"mod_grow_pct"} != 0 &&
        $inputHash_ref->{"mod_grow_by_size"} == 0 &&
	$inputHash_ref->{"mod_grow_by_pct"} == 0) {
	print "Should specify either MOD_GROW_BY_SIZE or MOD_GROW_BY_PCT when MOD_GROW_PCT is specified\n";
	print _usage();
	exit;
    } 

    if ($inputHash_ref->{"mod_shrink_by_size"} != 0 &&
        $inputHash_ref->{"mod_shrink_by_pct"} != 0) {
	print "Can specify only one of MOD_SHRINK_BY_SIZE or MOD_SHRINK_BY_PCT; Not Both\n";
	print _usage();
	exit;
    }

    if ($inputHash_ref->{"mod_shrink_pct"} != 0 &&
        $inputHash_ref->{"mod_shrink_by_size"} == 0 &&
	$inputHash_ref->{"mod_shrink_by_pct"} == 0) {
	print "Should specify either MOD_SHRINK_BY_SIZE or MOD_SHRINK_BY_PCT when MOD_SHRINK_PCT is specified\n";
	print _usage();
	exit;
    }

    if ($inputHash_ref->{"mod_clone_pct"} > 0 &&
	$inputHash_ref->{"operation_type"} =~ /modify/ &&
        (!defined($inputHash_ref->{"mgmt_ip"}) or
	 !defined($inputHash_ref->{"username"}) or
	 !defined($inputHash_ref->{"password"}) or
	 !defined($inputHash_ref->{"vserver"}) or
	 !defined($inputHash_ref->{"vol_name"}))) {
	print "When MOD_CLONE_PCT is specified, values for the following parameters ".
	      "should also be provided: MGMT_IP, USERNAME, PASSWORD, VSERVER_NAME, VOL_NAME\n";
	print _usage();
	exit;
    }

    if (-d $inputHash_ref->{"dir_path"} &&
	$inputHash_ref->{"mod_delete_pct"} +
	$inputHash_ref->{"mod_grow_pct"} +
	$inputHash_ref->{"mod_shrink_pct"} +
	$inputHash_ref->{"mod_overwrite_pct"} +
	$inputHash_ref->{"mod_punchhole_pct"} +
	$inputHash_ref->{"mod_clone_pct"} > 100) {
	print "The sum of modify percentages for delete, grow, shrink, ".
	      "overwrite, and clone should be less than 100\n";
	exit;
    }

	if(!defined($inputHash_ref->{incremental_iteration})) {
	$inputHash_ref->{index_file_path} = $inputHash_ref->{"dir_path"} . "/index_file_baseline" ;
	}
	else{
	    if($inputHash_ref->{"operation_type"} =~ /populate/ ){
	       $inputHash_ref->{index_file_path} = $inputHash_ref->{"dir_path"} . "/index_file_baseline" ;
		}   
		if($inputHash_ref->{"operation_type"} =~ /modify/ ){
           $inputHash_ref->{index_file_path} = $inputHash_ref->{"dir_path"} . "/index_file_incremental_" . $inputHash_ref->{incremental_iteration} ;
		}		   
	}
	
	
	
    if (!defined($inputHash_ref->{"operation_type"})) {
	print "Please specify an operation type via OPERATION_TYPE parameter\n";
	print _usage();
	exit;
    }

    print "\nSHAN: Path in create tree" . $inputHash_ref->{"dir_path"} . "\n";
    if ($inputHash_ref->{"operation_type"} eq "populate"){
	    if (!mkdir($inputHash_ref->{"dir_path"})){
		    print "Failed to create dir  : $!\n";
		    exit;
	    }
    }
  
    if ($inputHash_ref->{"operation_type"} eq "read" &&
        ($inputHash_ref->{"mod_create_pct"} != 0 ||
         $inputHash_ref->{"mod_delete_pct"} != 0 ||
         $inputHash_ref->{"mod_shrink_pct"} != 0 ||
         $inputHash_ref->{"mod_grow_pct"} != 0 ||
         $inputHash_ref->{"mod_overwrite_pct"} != 0 ||
         $inputHash_ref->{"mod_clone_pct"} != 0)) {
	print "Cannot specify modification parameters when the OPERATION_TYPE is \"read\"\n";
	print _usage();
	exit;
    }

    if ($inputHash_ref->{"operation_type"} =~ /modify/ &&
	$inputHash_ref->{"num_runs"} == 0) {
	$inputHash_ref->{"num_runs"} = 1;
    }

    _printOptions($inputHash_ref);
    
    print "\n";
}

sub run
{
    my $ARGV_ref = shift;
    my $num_args = shift;

    my (%inputHash, %dataSetInfoHash, $config_file);
 
    if ($num_args < 1) {
	print _usage();
	exit;
    }
    _getUserInput($ARGV_ref, $num_args, \%inputHash);

    if ($inputHash{"operation_type"} =~ /estimate/i) {
	_getEstimate(\%inputHash, \%dataSetInfoHash);
	exit;
    }

    if ($inputHash{"dir_path"} eq "") {
	print "Not a valid directory path: $inputHash{\"dir_path\"}\n";
	exit;
    }

	### shanthu to add index 
	my $index_file_path = $inputHash{"index_file_path"};# $inputHash{"dir_path"} . "/index_file_baseline.txt";
	open($index_file_baseline_handle, '>>', $index_file_path);
	

	
    if ($inputHash{"operation_type"} =~ /read/i) {
	if ($inputHash{"read_pct"} == 0) {
	    $inputHash{"read_pct"} = 10;
	}
	scanTree(\%inputHash, \%dataSetInfoHash);
	exit;
    }
    
    if ($inputHash{"operation_type"} =~ /populate/i) {
	#if (!_isEmptyDir($inputHash{"dir_path"})) {
	#    print "Directory path $inputHash{\"dir_path\"} is not empty\n";
	#    exit;
	#}
	createTree(\%inputHash, \%dataSetInfoHash);
    }
    
    if ($inputHash{"operation_type"} =~ /modify/i && 
	$inputHash{"num_runs"} > 0) {
	$dataSetInfoHash{"total_files_cnt"} = pow($inputHash{"dir_cnt"}, $inputHash{"dir_depth"}) * $inputHash{"file_cnt"};
	scanTree(\%inputHash, \%dataSetInfoHash);
    }
}

run(\@ARGV, $#ARGV + 1);

