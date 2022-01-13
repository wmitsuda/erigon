package commands

func newMultiIterator(smaller bool, fromIter, toIter BlockProvider) (BlockProvider, error) {
	// TODO: move this inside the closure; remove error from sig
	nextFrom, hasMoreFrom, err := fromIter()
	if err != nil {
		return nil, err
	}
	nextTo, hasMoreTo, err := toIter()
	if err != nil {
		return nil, err
	}

	return func() (uint64, bool, error) {
		if !hasMoreFrom && !hasMoreTo {
			return 0, false, nil
		}

		var blockNum uint64
		if !hasMoreFrom {
			blockNum = nextTo
		} else if !hasMoreTo {
			blockNum = nextFrom
		} else {
			blockNum = nextFrom
			if smaller {
				if nextTo < nextFrom {
					blockNum = nextTo
				}
			} else {
				if nextTo > nextFrom {
					blockNum = nextTo
				}
			}
		}

		// Pull next; it may be that from AND to contains the same blockNum
		if hasMoreFrom && blockNum == nextFrom {
			nextFrom, hasMoreFrom, err = fromIter()
			if err != nil {
				return 0, false, err
			}
		}
		if hasMoreTo && blockNum == nextTo {
			nextTo, hasMoreTo, err = toIter()
			if err != nil {
				return 0, false, err
			}
		}
		return blockNum, hasMoreFrom || hasMoreTo, nil
	}, nil
}
