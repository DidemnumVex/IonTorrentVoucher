# IonTorrentVoucher
Pipeline for splitting, assembling, and BLASTing barcode sequences from Ion Torrent 

This pipeline is being created for work done at Moss Landing Marine Laboratories Invertebrate Lab as part of our non-indigenous species identification project.  It is currently a work in progress, however, it is functional on our in-house system.  At this time, I would not expect it to be fully functional on any other system.  I would love to hear from folks doing similar work and work to tailor it if they are interested. 

**Dependencies:** Must have installed Qiime, Mira Assembler, Pandas, and Scipy (and associated dependencies thereof) and mysql.connector (Also a MySQL database with our in-house directory structure.  I am, however, planning to make xls input an option).  

This pipeline was built for high-throughput sequencing of tissue samples.  Tissues were extracted in 96-well plate format (one tissue sample per well) and the COI gene was amplified with PCR.  Barcodes were attached to indicate well.  The wells from the plate were pooled after well-indexing and a barcode was ligated to the pool to indicate plate.  Thirty-two 96-well plates were pooled into a Library and sequence in the Ion Torrent PGM with either a 400 bp or Hi-Q kit. 

The first half of the pipeline unzips, demultiplexes, and assembles contigs.  The second half of the pipeline does a BLAST search of the contigs against our in-house database and (yet to be completed) compares the result to the morphological ID for that specimen. 


My To-Do List for this pipeline: 

- [ ] Upload remaining scripts 
- [ ] Write user documentation to show 
    - 1) [ ] order of operations 
    - 2) [ ] command line arguments
    - 3) [ ] input file structure 
- [ ] Add logging feature with if statement to 'start from' section in case of crash 
