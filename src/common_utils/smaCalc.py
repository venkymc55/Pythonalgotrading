def calculateSMA(completeListOfNumbers, SMALegnth, currentDateAsAnIndexNumber):  # NOTE: Returns double
    runningSum = 0

    if currentDateAsAnIndexNumber - SMALegnth + 1 > -1:
        for num in range(currentDateAsAnIndexNumber - SMALegnth + 1, currentDateAsAnIndexNumber + 1):
            runningSum += completeListOfNumbers[num]
        return runningSum / SMALegnth
    else:
        return 0


def convertTimeToIndex(listOfTimes):
    result = []
    for index in range(len(listOfTimes)):
        # if listOfTimes[index] == time:
        result.append(index)
    return result


def SMAAboveCrossoverHasOccured(SMALegnthShort, SMALegnthLong, date1,
                                completeListOfNumbers):  # NOTE: returns boolean, dates are indicies
    if SMALegnthShort > SMALegnthLong:
        SMALegnthShort, SMALegnthLong = SMALegnthLong, SMALegnthShort

    if (calculateSMA(completeListOfNumbers, SMALegnthShort, date1)) > calculateSMA(completeListOfNumbers,
                                                                                       SMALegnthLong,
                                                                                       date1):  # If short SMA is originally < long SM  # But then becomes more
        return True  # an upward crossover has occured

    return False


def SMAAboveCrossunderHasOccured(SMALegnthShort, SMALegnthLong, date1,
                                 completeListOfNumbers):  # NOTE: returns boolean, dates are indicies
    if SMALegnthShort > SMALegnthLong:
        SMALegnthShort, SMALegnthLong = SMALegnthLong, SMALegnthShort

    if (calculateSMA(completeListOfNumbers, SMALegnthShort, date1)) < calculateSMA(completeListOfNumbers,
                                                                                       SMALegnthLong,
                                                                                       date1):  # If short SMA is originally < long SMA
        return True  # an upward crossover has occured

    return False
