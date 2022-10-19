"""A repeatable benchmark tool for ion-python implementation.

Usage:
    ion_python_benchmark_cli.py write
    ion_python_benchmark_cli.py read [--api <api>] [--warmups <int>] [--iterations <int>] <input_file>
    ion_python_benchmark_cli.py generate
    ion_python_benchmark_cli.py (-h | --help)

Command:
    write       Benchmark writing the given input file to the given output format(s). In order to isolate
    writing from reading, during the setup phase write instructions are generated from the input file
    and stored in memory. For large inputs, this can consume a lot of resources and take a long time
    to execute. This may be reduced by using the --limit option to limit the number of entries that
    are written. The cost of initializing the writer is included in each timed benchmark invocation.
    Therefore, it is important to provide data that closely matches the size of the data written by a
    single writer instance in the real world to ensure the initialization cost is properly amortized.

    read        First, re-write the given input file to the given output format(s) (if necessary), then
    benchmark reading the resulting log files. If this takes too long to complete, consider using
    the --limit option to limit the number of entries that are read. Specifying non-default settings
    for certain options will cause the input data to be re-encoded even if the requested format is the
    same as the format of the provided input. These options are --ion-length-preallocation and
    --ion-flush-period for input in the ion binary format. The cost of initializing the reader or
    DOM loader is included in each timed benchmark invocation. Therefore, it is important to provide
    data that closely matches the size of data read by a single reader/loader instance in the real
    world to ensure the initialization cost is properly amortized.

    generate    (EXPERIMENTAL) Generate random Ion data which can be used as input to the read/write commands.
    Data size, data type and the path of output file are required options. The specifications of three
    scalar types can be executed so far, decimal, string and timestamp. The command will generate approximately
    the amount of data requested, but the actual size of the generated may be slightly larger or smaller than
    requested. We don't implement this feature in this implementation. We rely on ion-java-benchmark-cli to
    achieve the same outcomes.

Options:
     -h, --help                          Show this screen.

     --api <api>                        The API to excise (simpleIon, iterator, nonBlocking). `simpleIon` refer to
                                        simpleIon's load method. `iterator` refers to simpleIon's iterator type got by
                                        setting `parse_eagerly` to false. `nonBlocking` refer to ion-python's event
                                        based non-blocking API. Default to `simpleIon`.

     -w --warmups <int>                 Number of benchmark warm-up iterations. [default: 10]

     -i --iterations <int>              Number of benchmark iterations. [default: 10]


"""
import timeit
import tracemalloc
from pathlib import Path

from docopt import docopt
from tabulate import tabulate

from amazon.ionbenchmark.API import API

BYTES_TO_MB = 1024 * 1024


def generate_simpleion_load_test_code(file, single_value=False, emit_bare_values=False):
    return f'with open("{file}", "br") as fp: ion.load(fp, single_value={single_value}, emit_bare_values={emit_bare_values});'


def generate_simpleion_load_setup(gc=True):
    rtn = 'import amazon.ion.simpleion as ion'
    if gc:
        rtn += '; import gc; gc.enable();'
    return rtn


def read_micro_benchmark_simpleion(iterations, warmups, file=None):
    file_size = Path(file).stat().st_size / BYTES_TO_MB

    setup_without_gc = generate_simpleion_load_setup(gc=False)
    # GC refers to reference cycles, not reference count
    setup_with_gc = generate_simpleion_load_setup(gc=True)

    test_code = generate_simpleion_load_test_code(file, emit_bare_values=False)
    test_code_without_wrapper = generate_simpleion_load_test_code(file, emit_bare_values=True)

    # warm up
    timeit.timeit(stmt=test_code, setup=setup_with_gc, number=warmups)
    timeit.timeit(stmt=test_code, setup=setup_without_gc, number=warmups)
    timeit.timeit(stmt=test_code_without_wrapper, setup=setup_without_gc, number=warmups)

    # iteration
    result_with_gc = timeit.timeit(stmt=test_code, setup=setup_with_gc, number=iterations) / iterations
    result_without_gc = timeit.timeit(stmt=test_code, setup=setup_without_gc, number=iterations) / iterations
    result_with_raw_value = \
        timeit.timeit(stmt=test_code_without_wrapper, setup=setup_without_gc, number=iterations) / iterations

    return file_size, result_with_gc, result_without_gc, result_with_raw_value


def read_micro_benchmark_iterator(iterations, warmups, file=None):
    pass


def read_micro_benchmark_non_blocking(iterations, warmups, file=None):
    pass


def read_micro_benchmark_and_profiling(read_micro_benchmark_function, iterations, warmups, file):
    if not file:
        raise Exception("Invalid file: file can not be none.")
    if not read_micro_benchmark_function:
        raise Exception("Invalid micro benchmark function: micro benchmark function can not be none.")

    tracemalloc.start()
    file_size, result_with_gc, result_without_gc, result_with_raw_value = \
        read_micro_benchmark_function(iterations=iterations, warmups=warmups, file=file)
    memory_usage_peak = tracemalloc.get_traced_memory()[1] / BYTES_TO_MB
    tracemalloc.stop()
    # calculate metrics
    garbage_collection_time = result_with_gc - result_without_gc
    conversion_time = result_without_gc - result_with_raw_value
    read_generate_report("{:.2e}".format(file_size),
                         "{:.2e}".format(result_with_gc),
                         "{:.2e}".format(result_without_gc),
                         "{:.2e}".format(garbage_collection_time) if garbage_collection_time > 0 else 0,
                         "{:.2%}".format(garbage_collection_time / result_with_gc) if garbage_collection_time > 0 else "0%",
                         "{:.2e}".format(conversion_time) if conversion_time > 0 else 0,
                         "{:.2%}".format(conversion_time / result_with_gc) if conversion_time > 0 else "0%",
                         "{:.2e}".format(memory_usage_peak))


def read_generate_report(file_size, total_time, execution_time, garbage_collection_time, gc_time_percentage,
                         conversion_time, wrapper_time_percentage, memory_usage_peak):
    table = [['file_size (MB)', 'total_time (s)', 'execution_time (s)', 'garbage_collection_time (s)',
              'garbage_collection_time/total_time (%)', 'conversion_time (s)', 'conversion_time/total_time (%)',
              'memory_usage_peak (MB)'],
             [file_size, total_time, execution_time, garbage_collection_time, gc_time_percentage,
              conversion_time, wrapper_time_percentage, memory_usage_peak]]
    print(tabulate(table, tablefmt='fancy_grid'))


def ion_python_benchmark_cli(arguments):
    if not arguments['<input_file>']:
        raise Exception('Invalid input file')
    file = arguments['<input_file>']
    iterations = int(arguments['--iterations'])
    warmups = int(arguments['--warmups'])

    if arguments['read']:
        api = arguments['--api']
        if not api or api == API.SIMPLE_ION.value:
            read_micro_benchmark_function = read_micro_benchmark_simpleion
        elif api == API.ITERATOR.value:
            read_micro_benchmark_function = read_micro_benchmark_iterator
        elif api == API.NON_BLOCKING.value:
            read_micro_benchmark_function = read_micro_benchmark_non_blocking
        else:
            raise Exception(f'Invalid API option {api}.')

        read_micro_benchmark_and_profiling(read_micro_benchmark_function, iterations, warmups, file)

    elif arguments['write']:
        print('Write feature is not supported yet')
    elif arguments['generate']:
        print('Generate feature is not supported yet')


if __name__ == '__main__':
    ion_python_benchmark_cli(docopt(__doc__, help=True))
