#!/usr/bin/perl

use strict;
use warnings;
use RDF::Trine;
use File::Spec;
use File::Path qw(make_path);
use Getopt::Long;

my %namespaces;
my $in		= 'ntriples';
my $debug	= 0;
my $result	= GetOptions ("in=s" => \$in, "define=s" => \%namespaces, "D=s" => \%namespaces, "verbose" => \$debug);

unless (@ARGV) {
	print <<"END";
Usage: $0 data.rdf http://base /path/to/www/
END
	exit;
}

my $file	= shift or die "An RDF filename must be given";
my $url		= shift or die "A URL base must be given";
my $base	= shift or die "A path to the base URL must be given";
my %files;

if ($url =~ m<[/]$>) {
	chop($url);
}

open( my $fh, '<:utf8', $file ) or die $!;

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
		next unless ($uri =~ m<^${url}/source/([^/]+)/dataset/([^/]+)/version/([^/]+)/(.*)>);
		my ($source, $dataset, $version, $thing)	= ($1, $2, $3, $4);
		my $path		= File::Spec->catdir( $base, source => $source, 'file', dataset => $dataset, version => $version );
		make_path( $path );
		my $filename	= File::Spec->catfile( $path, "${thing}.nt" );
		$files{ $filename }++;
		unless (-r $filename) {
			warn "Creating $filename...\n" if ($debug);
		}
		open( my $fh, '>>:utf8', $filename ) or next;
		$serializer->serialize_iterator_to_file( $fh, RDF::Trine::Iterator::Graph->new([$st]) );
	}
}


my %serializers;
foreach my $s (qw(rdfxml turtle)) {
	$serializers{ $s }	= RDF::Trine::Serializer->new( $s, namespaces => \%namespaces );
}

my %ext	= ( rdfxml => 'rdf', turtle => 'ttl' );
foreach my $filename (keys %files) {
	open( my $fh, '<:utf8', $filename ) or do { warn $!; next };
	my $parser	= RDF::Trine::Parser->new('ntriples');
	my $model	= RDF::Trine::Model->temporary_model;
	$parser->parse_file_into_model( $url, $fh, $model );
	while (my($name, $s) = each(%serializers)) {
		my $ext	= $ext{ $name };
		my $outfile	= $filename;
		$outfile	=~ s/[.]nt/.$ext/;
		warn "Creating $outfile...\n" if ($debug);
		open( my $out, '>:utf8', $outfile ) or do { warn $!; next };
		$s->serialize_model_to_file( $out, $model );
	}
}
