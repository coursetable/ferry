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
