nextflow.enable.dsl=2

include { MAKE_GFF } from './modules/make_gff'
include { RUN_ROSE } from './modules/run_rose'
include { SORT_BED } from './modules/sort_bed'
include { BED_TO_MACS } from './modules/bed_to_macs'
include { REDUCE_BED } from './modules/reduce_bed'
include { RENAME_PNG } from './modules/rename_png'
include { ASSIGN_GENES } from './modules/assign_genes'
include { BED_TO_BIGBED } from './modules/bed_to_bigbed'
include { ADD_ISLAND_NAMES } from './modules/add_island_names'

ch_islands_control_file = Channel.fromPath( params.islands_control_file ).ifEmpty( null )
ch_annotation_file      = Channel.fromPath( params.annotation_file )
ch_bambai_pair          = Channel.fromPath( params.bambai_pair ).toList()
ch_chrom_length_file    = Channel.fromPath( params.chrom_length_file )
ch_islands_file         = Channel.fromPath( params.islands_file )


workflow  {

    MAKE_GFF(
        ch_islands_file,
        ch_islands_control_file
    )

    RUN_ROSE(
        ch_bambai_pair,
        ch_annotation_file,
        MAKE_GFF.out.gff_file
    )

    SORT_BED(
        RUN_ROSE.out.gateway_super_enhancers_bed
    )

    BED_TO_MACS(
        SORT_BED.out.sorted_file
    )

    REDUCE_BED(
        SORT_BED.out.sorted_file
    )

    RENAME_PNG(
        RUN_ROSE.out.plot_points_pic,
        ch_bambai_pair.map{ tuple -> tuple[0] }
    )

    ASSIGN_GENES(
        ch_annotation_file,
        BED_TO_MACS.out.output_file
    )

    BED_TO_BIGBED(
        ch_chrom_length_file,
        REDUCE_BED.out.output_file,
        ch_bambai_pair.map{ tuple -> tuple[0] }
    )

    ADD_ISLAND_NAMES(
        ASSIGN_GENES.out.result_file.toList(),
        ch_bambai_pair.map{ tuple -> tuple[0] }
    )


}
