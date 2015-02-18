import luigi

# Test pipeline for the eInfrastructures workshop
# about title: "A RNA-seq pipeline to show some of bpipe's features"
# inputs "fq.gz" : "This pipeline expexts a fastq readfile (.fq)"

#-----------------------------
# Configuration variables
#-----------------------------

#Location of Bowtie2 index
BWT2_INDEX="~/bpipe_tutorial/genome/4"

#Location of the genome sequence in FASTA format
GENOME_FA="~/bpipe_tutorial/genome/4.fa"

GENOME_GTF="~/bpipe_tutorial/genome/genes.gtf"

#-----------------------------
# Pipeline stages
#-----------------------------



#-----------------------------
# Configuration variables
#-----------------------------

#Location of Bowtie2 index
BWT2_INDEX="~/bpipe_tutorial/genome/4"

#Location of the genome sequence in FASTA format
GENOME_FA="~/bpipe_tutorial/genome/4.fa"

GENOME_GTF="~/bpipe_tutorial/genome/genes.gtf"
#-----------------------------
# Pipeline stages
#-----------------------------

#tophat = {
#	# We store the name of the sample in a global variable to be used to define sample
#	# specific output folders. The variable will be accessible by all downstream stages.	
#	# (branch.name will change if we introduce more branches, so we can't rely on it later)
#	branch.sample = branch.name
#
#	# Tophat (like cufflinks) has no file target and writes to a folder. To make sure bpipe
#	# understands this we specify the output folder and..
#	output.dir = branch.sample + "/tophat"
#	
#	# specify the name of one of the output files to have a verifyable output from the stage.
#	produce("accepted_hits.bam") {
#		exec "tophat -o $output.dir -p $threads $BWT2_INDEX $input1 $input2 2> /dev/null","tophat"
#	}
#}

class TopHat(luigi.Task):
    branch_sample = luigi.Parameter()
    output_dir = self.branch_sample + '/tophat'

    bwt2_index = luigi.Parameter()
    input1 = luigi.Parameter()
    input2 = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.output_dir + '/accepted_hits.bam')

    def run(self):
        self.x(['tophat',
                '-o', self.output_dir,
                '-p', self.threads,
                self.get_input('bwt2_index'),
                self.get_input('input1'),
                self.get_input('input2'),
                '2> /dev/null'])



#cufflinks = {
#
#	output.dir = branch.sample + "/cufflinks"
#	
#	produce("transcripts.gtf") {
#		exec "cufflinks -p $threads -o $output.dir -L $branch.sample -G $GENOME_GTF -u -b $GENOME_FA $input 2> $output.dir" + "/log.txt","cufflinks"
#	}
#
#}

class CuffLinks(luigi.Task):
    output_dir = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.output_dir + '/transcripts.gtf')

    def run(self):
        self.x(['cufflinks',
                '-p', self.threads,
                '-o', self.output_dir,
                '-L', self.branch_sample,
                '-G', GENOME_GTF,
                '-u',
                '-b', GENOME_FA,
                self.input, '2>',
                self.output_dir + '/log.txt'])

#samtools_index = {
#
#	exec "samtools index $input"
#	forward input
#}

class SamToolsIndex(luigi.Task):
    input = luigi.Parameter()

    def output():
        return {'output' : self.get_input('input')}

    def run(self):
        self.x(['samtools index', 
                self.get_input('input').path])


#samtools_flagstat = {
#	
#	output.dir = branch.sample + "/stats"
#
#	produce("alignment_statistics.txt") {
#		exec "samtools flagstat $input > $output"
#	}
#}

class SamToolsFlagstat(luigi.Task):
    branch_sample = luigi.Parameter()
    output_dir = self.branch_sample + '/stats'

    input = luigi.Parameter()

    def output(self):
        return {'output' : luigi.LocalTarget(self.output_dir + '/alignment_statistics.txt')}

    def run(self):
        self.x(['samtools flagstat',
                self.get_input('input').path,
                '>',
                self.putput().path])

#-----------------------------
# Actual workflow
#-----------------------------
	
#run { "%.*.fq.gz" * [
#		tophat + [ samtools_index + samtools_flagstat,  cufflinks ]
#	]
#}

class RnaSeqPipeline(luigi.Task):
    threads = luigi.Parameter()
    
    def requires(self):
    branch_sample = luigi.Parameter()
    output_dir = self.branch_sample + '/tophat'

    bwt2_index = luigi.Parameter()
    input1 = luigi.Parameter()
    input2 = luigi.Parameter()


        tophat = TopHat(
                threads = self.threads,
                bwt2_index = { 'upstream' : { 'task' : 'NA',
                                              'port' : 'NA' } },
                input1 = { 'upstream' : { 'task' : 'NA',
                                          'port' : 'NA' } },
                input2 = { 'upstream' : { 'task' : 'NA',
                                          'port' : 'NA' } },
                branch_sample = 'NA',
        )
        samtools_index = SamToolsIndex(

        )
        samtools_flagstat = SamToolsFlagstat(
            input = { 'upstream' : { 'task' : samtools_index,
                                     'port' : 'output' } }, 
            branch_sample = 'NA',
        )
        cufflinks = CuffLinks(
                threads = self.threads
        )
        return cufflinks

    def output(self):
        return self.intput()


# If this file is run as a script, hand over to luigi!
if __name__ == '__main__':
    luigi.run()

