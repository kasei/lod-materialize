#!/usr/bin/perl

=head1 NAME

lod-materialize.pl - Materialize the files necessary to host slash-based linked data.

=head1 SYNOPSIS

 lod-materialize.pl [OPTIONS] data.rdf http://base /path/to/www
=head1 DESCRIPTION

This script will materialize the necessary files for serving static linked data.
Given an input file data.rdf, this script will find all triples that use a URI
as subject or object that contains the supplied base URI, and serialize the
matching triples to the appropriate files for serving as linked data.

For example, using the input RDF:

 @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
 @prefix db: <http://dbpedia.org/resource/> .
 @prefix prop: <http://dbpedia.org/property/> .
 @prefix dbo: <http://dbpedia.org/ontology/> .
 
 db:Berlin a dbo:City ;
     rdfs:label "Berlin"@en ;
     prop:population 3431700 .
 db:Deutsche_Bahn dbo:locationCity db:Berlin .

Invoking this command:

 lod-materialize.pl -i=turtle data.ttl http://dbpedia.org /var/www

Will produce the files:

 /var/www/data/Berlin.rdf
 /var/www/data/Berlin.ttl
 /var/www/data/Deutsche_Bahn.rdf
 /var/www/data/Deutsche_Bahn.ttl

The process of mapping URIs to files on disk can be configured using the command
line OPTIONS 'uripattern' and 'filepattern':

 lod-materialize.pl --uripattern="/resource/(.*)" --filepattern="/page/\\1" data.rdf http://dbpedia.org /var/www

This will create the files:

 /var/www/page/Berlin.rdf
 /var/www/page/Berlin.ttl
 /var/www/page/Deutsche_Bahn.rdf
 /var/www/page/Deutsche_Bahn.ttl

=head1 OPTIONS

Valid command line options are:

=over 4

=item * -in=FORMAT

=item * -i=FORMAT

Specify the name of the RDF format used by the input file. Defaults to "ntriples".

=item * -out=FORMAT,FORMAT

=item * -o=FORMAT,FORMAT

Specify a comma-seperated list of RDF formats used for serializing the output
files. Defaults to "rdfxml,turtle,ntriples".

=item * --define ns=URI

=item * -D ns=URI

Specify a namespace mapping used by the serializers.

=item * --verbose

Print information about file modifications to STDERR.

=item * -n

=item * --dryrun

Performa dry-run without modifying any files on disk.

=item * --uripattern=PATTERN

Specifies the URI pattern to match against URIs used in the input RDF. URIs in
the input RDF are matched against this pattern appended to the base URI
(http://base above).

=item * --filepattern=PATTERN

Specifies the path template to use in constructing data filenames. This pattern
will be used to construct an absolute filename by interpreting it relative to
the path specified for the document root (/path/to/www above).

=item * --apache

Print the Apache configuration needed to serve the produced RDF files as linked
data. This includes setting Multiview for content negotiation, the media type
registration for RDF files and mod_rewrite rules for giving 303 redirects from
resource URIs to the content negotiated data URIs.

=back

=cut

use strict;
use warnings;
use RDF::Trine;
use File::Spec;
use File::Path 2.06 qw(make_path);
use Getopt::Long;
use Data::Dumper;

my %namespaces;
my $in		= 'ntriples';
my $out		= 'rdfxml,turtle,ntriples';
my $matchre	= q</resource/(.*)>;
my $outre	= '/data/$1';
my $dryrun	= 0;
my $debug	= 0;
my $apache	= 0;
my $result	= GetOptions (
	"in=s"			=> \$in,
	"out=s"			=> \$out,
	"define=s"		=> \%namespaces,
	"D=s"			=> \%namespaces,
	"uripattern=s"	=> \$matchre,
	"filepattern=s"	=> \$outre,
	"verbose"		=> \$debug,
	"n"				=> \$dryrun,
	"apache"		=> \$apache,
);

unless (@ARGV) {
	print <<"END";
Usage: $0 [OPTIONS] data.rdf http://base /path/to/www/
END
	exit;
}

my $file	= shift or die "An RDF filename must be given";
my $url		= shift or die "A URL base must be given";
my $base	= shift or die "A path to the base URL must be given";
my @out		= split(',', $out);
my %files;
my %paths;

if ($url =~ m<[/]$>) {
	chop($url);
}

if ($debug) {
	warn "Input file     : $file\n";
	warn "Input format   : $in\n";
	warn "Output formats : " . join(', ', @out) . "\n";
	warn "URL Pattern    : $matchre\n";
	warn "File Pattern   : $outre\n";
}

open( my $fh, '<:utf8', $file ) or die "Can't open RDF file $file: $!";

my $parser	= RDF::Trine::Parser->new( $in );
my $serializer	= RDF::Trine::Serializer->new( 'ntriples', namespaces => \%namespaces );

$parser->parse_file( 'http://base/', $fh, \&handle_triple );

sub handle_triple {
	my $st	= shift;
# 	warn "parsing triple: " . $st->as_string . "\n";
	foreach my $pos (qw(subject object)) {
		my $obj	= $st->$pos();
		next unless ($obj->isa('RDF::Trine::Node::Resource'));
		my $uri	= $obj->uri_value;
		next unless (my @matched = $uri =~ qr/^${url}$matchre/);
# 		my ($source, $dataset, $version, $thing)	= ($1, $2, $3, $4);
# 		my $path		= File::Spec->catdir( $base, source => $source, 'file', dataset => $dataset, version => $version );
		
		my $file	= $outre;
		foreach my $i (1 .. scalar(@matched)) {
			while ($file =~ m/(\$|\\)$i/) {
				$file	=~ s/(\$|\\)$i/$matched[$i-1]/;
			}
		}
		(undef, my $path, my $thing)	= File::Spec->splitpath( File::Spec->catfile( $base, $file ) );
		unless ($paths{ $path }) {
			warn "Creating directory $path ...\n" if ($debug);
			$paths{ $path }++;
			unless ($dryrun) {
				make_path( $path );
			}
		}
		
		my $filename	= File::Spec->catfile( $path, "${thing}.nt" );
		unless ($files{ $filename }) {
			$files{ $filename }++;
			unless (-r $filename) {
				warn "Creating file $filename ...\n" if ($debug);
			}
		}
		unless ($dryrun) {
			open( my $fh, '>>:utf8', $filename ) or next;
			$serializer->serialize_iterator_to_file( $fh, RDF::Trine::Iterator::Graph->new([$st]) );
		}
	}
}


my %serializers;
foreach my $s (@out) {
	$serializers{ $s }	= RDF::Trine::Serializer->new( $s, namespaces => \%namespaces );
}

my %ext	= ( rdfxml => 'rdf', turtle => 'ttl', ntriples => 'nt' );
foreach my $filename (sort keys %files) {
	my $parser	= RDF::Trine::Parser->new('ntriples');
	my $store	= RDF::Trine::Store::DBI->temporary_store;
	my $model	= RDF::Trine::Model->new( $store );
	warn "Parsing file $filename ...\n" if ($debug);
	unless ($dryrun) {
		open( my $fh, '<:utf8', $filename ) or do { warn $!; next };
		$parser->parse_file_into_model( $url, $fh, $model );
	}
	while (my($name, $s) = each(%serializers)) {
		my $ext	= $ext{ $name };
		my $outfile	= $filename;
		$outfile	=~ s/[.]nt/.$ext/;
		warn "Creating file $outfile ...\n" if ($debug);
		unless ($dryrun) {
			open( my $out, '>:utf8', $outfile ) or do { warn $!; next };
			$s->serialize_model_to_file( $out, $model );
		}
	}
	
	unless (exists $serializers{'ntriples'}) {
		warn "Removing file $filename ...\n" if ($debug);
		unless ($dryrun) {
			unlink($filename);
		}
	}
}

if ($apache) {
	print "\n# Apache Configuration:\n";
	print "#######################\n";
	my $match	= substr($matchre,1);
	my $redir	= $outre;
	$redir		=~ s/\\(\d+)/\$$1/g;
	print <<"END";
Options +MultiViews
AddType text/turtle .ttl
AddType text/plain .nt
AddType application/rdf+xml .rdf

RewriteEngine On
RewriteBase /
RewriteRule ^${match}\$ $redir [R=303,L]
#######################

END
}
