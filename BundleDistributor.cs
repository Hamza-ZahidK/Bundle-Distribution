using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Linq;
using ProductivMD.Server.Compensation.Enums;

namespace ProductivMD.Server.Compensation
{
    public class BundleDistributor
    {
        public AccrualPeriodDecipher Decipher { get; private set; }

        public BundleDistributor(AccrualPeriodDecipher decipher)
        {
            this.Decipher = decipher ?? throw new ArgumentNullException(nameof(decipher));
        }

        /// <summary>
        /// To distribute bundles based on specified parameters
        /// </summary>
        public BundleDistribution Distribute(IEnumerable<KeyValuePair<DateTime, decimal?>> values, IEnumerable<decimal> tierBoundaries, IEnumerable<decimal> conversionFactors, DateRange bundlePeriod, bool cumulative)
        {
            // Get reset dates, accrual interval, and other relevant information from the AccrualPeriodDecipher
            var resetDates = this.Decipher.ResetDates;
            var accrualIntervalMonths = this.Decipher.Interval.TranslateToMonths();
            var accrualIntervalDividend = 12 / accrualIntervalMonths;
            var accrualPeriods = this.Decipher.DecipherAccrualPeriods(bundlePeriod, cumulative);

            // Determine the end date of the last full interval period
            var lastResetDate = resetDates.Any() ? resetDates.Last() : this.Decipher.DateWindow.StartDate;
            var lastFullIntervalPeriodEndDate = lastResetDate.AddMonths(accrualIntervalMonths).AddDays(-1);

            // Retrieve accrual periods within the report window
            var reportAccrualPeriods = this.Decipher.DecipherAccrualPeriods(new DateRange(this.Decipher.DateWindow.StartDate, lastFullIntervalPeriodEndDate), false).ToList();
            var conversionRange = new NumericRange(Enumerable.Min(conversionFactors), Enumerable.Max(conversionFactors));
            var months = this.Decipher.MonthStartDates;

            // Calculate accrual period distributions
            var accrualPeriodDistributions = accrualPeriods.Select(ap =>
            {
                // Calculate the ratio for the current accrual period
                var ratio = reportAccrualPeriods
                    .Where(rap => rap.StartDate <= ap.EndDate && rap.EndDate >= ap.StartDate)
                    .Select(rap =>
                    {
                        // Calculate the relative dates and days within the period
                        var relativeStartDate = Enumerable.Max(new DateTime[] { rap.StartDate, ap.StartDate });
                        var relativeEndDate = Enumerable.Min(new DateTime[] { rap.EndDate, ap.EndDate });
                        var relativeDaysInPeriod = Convert.ToDecimal(relativeEndDate.Subtract(relativeStartDate).TotalDays) + 1;
                        var daysInReportPeriod = Convert.ToDecimal(rap.EndDate.Subtract(rap.StartDate).TotalDays) + 1;

                        return relativeDaysInPeriod / daysInReportPeriod / accrualIntervalDividend;
                    })
                    .Sum();

                // Adjust tier boundaries based on the calculated ratio
                var adjustedTierBoundaries = tierBoundaries.OrderBy(b => b).Select(b => (decimal?)(b * ratio)).Prepend(null).Append(null);
                // Create tier bounds based on adjusted boundaries
                var tierBounds = adjustedTierBoundaries.SkipLast(1).Zip(adjustedTierBoundaries.Skip(1), (lower, upper) => new NumericRange(lower, upper));
                // Assemble tier distributions based on tier bounds, conversion factors, and other parameters
                var tierDistributions = tierBounds.Zip(conversionFactors, (bounds, multipler) => AssembleTierDistribution(ap, bounds, multipler, months, values)).ToList();

                return new TieredAccrualPeriodDistribution(ap, tierDistributions);
            });

            var flattenedDistributions = accrualPeriodDistributions.Select(periodDist => FlattenDistribution(periodDist)).ToList();
            // Create and return the final BundleDistribution
            return new BundleDistribution(bundlePeriod, conversionRange, accrualPeriodDistributions, flattenedDistributions);
        }

        /// <summary>
        /// To fill distribution gaps in a collection of distributions
        /// </summary>
        public IEnumerable<BundleDistribution> FillDistributionGaps(IEnumerable<BundleDistribution> distributions, bool cumulative)
        {
            var distributionPeriods = distributions.Select(d => d.DistributionPeriod);
            var gapPeriods = this.Decipher.DecipherGaps(distributionPeriods);
            var emptyDistributions = gapPeriods.Select(gapPeriod => CreateEmptyDistribution(this.Decipher, gapPeriod, cumulative));
            return (emptyDistributions.Any()) ? distributions.Concat(emptyDistributions).OrderBy(b => b.DistributionPeriod.EndDate) : distributions;
        }

        /// <summary>
        /// To create an empty distribution for a specific date range
        /// </summary>
        private static BundleDistribution CreateEmptyDistribution(AccrualPeriodDecipher decipher, DateRange dateWindow, bool cumulative)
        {
            var accrualPeriods = decipher.DecipherAccrualPeriods(dateWindow, cumulative);
            var distributionMonths = decipher.MonthStartDates;
            var emptyMonthDistribution = Enumerable.Range(0, distributionMonths.Count()).Select(i => (decimal?)null);
            var emptyTierDistribution = new TierDistribution(new NumericRange(null, null), null, emptyMonthDistribution);
            var accrualPeriodDistributions = accrualPeriods.Select(ap => new TieredAccrualPeriodDistribution(ap, new[] { emptyTierDistribution }));
            var emptyFlattenedDistributions = accrualPeriods.Select(ap => new AccrualPeriodDistribution(ap, emptyMonthDistribution));
            return new BundleDistribution(dateWindow, null, accrualPeriodDistributions, emptyFlattenedDistributions);
        }

        /// <summary>
        /// To assemble a tier distribution based on accrual period, bounds, conversion factor, months, and values
        /// </summary>
        private static TierDistribution AssembleTierDistribution(DateRange accrualPeriod, NumericRange bounds, decimal conversionFactor, IEnumerable<DateTime> months, IEnumerable<KeyValuePair<DateTime, decimal?>> values)
        {
            var firstReportMonth = months.First();
            var lastReportMonth = months.Last();
            var applicableWrvus = values.Where(v => v.Key >= accrualPeriod.StartDate && v.Key <= accrualPeriod.EndDate);

            var prePeriodMonthCount = ((accrualPeriod.StartDate.Year - firstReportMonth.Year) * 12) + accrualPeriod.StartDate.Month - firstReportMonth.Month;
            var periodMonthCount = ((accrualPeriod.EndDate.Year - accrualPeriod.StartDate.Year) * 12) + accrualPeriod.EndDate.Month - accrualPeriod.StartDate.Month + 1;
            var postPeriodMonthCount = ((lastReportMonth.Year - accrualPeriod.EndDate.Year) * 12) + lastReportMonth.Month - accrualPeriod.EndDate.Month;

            var prePeriodBreakdown = Enumerable.Range(0, prePeriodMonthCount).Select(i => (decimal?)null);
            var periodTierBreakdown = DistributeAcrossTier(bounds, months.Skip(prePeriodMonthCount).Take(periodMonthCount), applicableWrvus).ToList();
            var postPeriodBreakdown = Enumerable.Range(0, postPeriodMonthCount).Select(i => (decimal?)null);

            var monthDistribution = prePeriodBreakdown.Concat(periodTierBreakdown).Concat(postPeriodBreakdown);
            return new TierDistribution(bounds, conversionFactor, monthDistribution);
        }

        /// <summary>
        /// To distribute values across tiers within a specific range of months
        /// </summary>
        private static IEnumerable<decimal?> DistributeAcrossTier(NumericRange bounds, IEnumerable<DateTime> months, IEnumerable<KeyValuePair<DateTime, decimal?>> values)
        {
            var periodCounter = 0M;
            var tierLowerBound = bounds.LowerBound ?? decimal.MinValue;
            var tierUpperBound = bounds.UpperBound ?? decimal.MaxValue;

            return months.Select(m =>
            {
                decimal? result = null;
                var monthValuePairs = values.Where(v => v.Key.Year == m.Year && v.Key.Month == m.Month);
                var monthSum = monthValuePairs.Any() ? monthValuePairs.Sum(v => v.Value) : null;

                if (monthSum != null)
                {
                    var periodCounterPlusMonth = periodCounter + monthSum.Value;
                    var periodRange = new NumericRange(Math.Min(periodCounter, periodCounterPlusMonth), Math.Max(periodCounter, periodCounterPlusMonth));

                    result = periodRange.Overlaps(bounds)
                        ? Math.Min(periodCounterPlusMonth, tierUpperBound) - Math.Max(periodCounter, tierLowerBound)
                        : 0M;

                    periodCounter = periodCounterPlusMonth;
                }

                return result;
            });
        }

        /// <summary>
        /// To flatten tiered accrual period distribution into a single accrual period distribution
        /// </summary>
        private static AccrualPeriodDistribution FlattenDistribution(TieredAccrualPeriodDistribution distribution)
        {
            var firstTierDist = distribution.TierDistributions.First().ConvertedMonthDistribution;
            var onGoingDistribution = firstTierDist;
            var flattenedPeriodDistributions = distribution.TierDistributions
                .Skip(1)
                .Select(tierDist =>
                {
                    var currentMonthDist = tierDist.ConvertedMonthDistribution;
                    return onGoingDistribution = onGoingDistribution.Zip(currentMonthDist, (a, b) => (a.HasValue || b.HasValue) ? (a ?? 0M) + (b ?? 0M) : (decimal?)null);
                })
                .Prepend(firstTierDist);

            return new AccrualPeriodDistribution(distribution.AccrualPeriod, flattenedPeriodDistributions.Last());
        }
    }
}
