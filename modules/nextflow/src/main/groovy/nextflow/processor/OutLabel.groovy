package nextflow.processor

import groovy.transform.CompileStatic

@CompileStatic
class OutLabel {

    final String label
    final double weight

    OutLabel( Map params ) {

        if (!params.containsKey('label')) {
            throw new IllegalStateException("OutLabel has no label")
        } else if (params.label instanceof Closure) {
            label = (params.label as Closure).call()
        } else {
            label = params.label
        }

        if (!params.containsKey('weight')) {
            weight = 2.0
        } else if (params.weight instanceof Closure) {
            weight = (params.weight as Closure).call()
        } else {
            weight = params.weight as double
        }

        if ( weight < 1.0 ) {
            throw new IllegalStateException( "OutLabel weight cannot be below 1.0, was: $weight for label: $label" )
        }

    }

    Map toMap(){
        [
            label : label,
            weight : weight
        ]
    }

}
