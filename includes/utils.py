from functools import reduce

def merge_overlapping(sets):
    """
    Given a list of sets, merge sets with
    a nonempty intersection until all sets
    are disjoint

    Parameters
    ----------
    sets: input list of sets

    Returns
    -------
    sets: output list of merged sets

    """

    is_merged = True

    while is_merged:

        is_merged = False
        temp_merged = []

        while len(sets) > 0:

            common, rest = sets[0], sets[1:]
            sets = []

            for x in rest:

                if x.isdisjoint(common):
                    sets.append(x)

                else:
                    is_merged = True
                    common |= x

            temp_merged.append(common)

        sets = temp_merged

    return sets


def elementwise_sum(a,b):
    """
    Given two lists of equal length, return
    a list of elementwise sums

    Parameters
    ----------
    a: list
        first list
    b: list
        second list

    Returns
    -------
    sums: elementwise sums

    """
    
    assert len(a)==len(b), "a and b must have same size"
    
    return [sum(x) for x in zip(a, b)]

def category_average(categories):
    """
    Given a list-like of n category counts, 
    aggregate and return the average where each 
    category denotes counts of [1,2,...n]

    Parameters
    ----------
    categories: list-like of lists
        categories

    Returns
    -------
    average: average category
    n: total number of responses

    """
    
    if len(categories) == 0:
        return None, None
    
    categories = reduce(elementwise_sum, categories)
    
    if sum(categories) == 0:
        return None, None
    
    categories_sum = sum([categories[i]*(i+1) for i in range(len(categories))])
    
    n = sum(categories)
    
    average = categories_sum/n
    
    return average, n