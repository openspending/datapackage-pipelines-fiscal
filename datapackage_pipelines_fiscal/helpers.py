def run_after_yielding_elements(resource_iterator, callback):
    '''Yields all iterator's elements, runs callback, and stops iteration.

    This method is useful when you want to run your processor's code after all
    previous processors have finished, but block subsequent processors from
    running before you're finished.
    '''
    while True:
        try:
            yield next(resource_iterator)
        except StopIteration:
            callback()
            raise
