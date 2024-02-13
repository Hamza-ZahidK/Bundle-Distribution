using System;
using System.Collections.Generic;
using System.Linq;

namespace ProductivMD.Server.Compensation
{
    public class BundleDistribution
    {
        public DateRange DistributionPeriod { get; private set; }

        public NumericRange ConversionRange { get; private set; }

        public IEnumerable<TieredAccrualPeriodDistribution> TieredDistributions { get; private set; }

        public IEnumerable<AccrualPeriodDistribution> FlattenedDistributions { get; private set; }

        public bool IsEmpty => (this.ConversionRange == null);

        internal BundleDistribution(DateRange distributionPeriod, NumericRange conversionRange, IEnumerable<TieredAccrualPeriodDistribution> distributions, IEnumerable<AccrualPeriodDistribution> flattenedDistributions)
        {
            this.DistributionPeriod = distributionPeriod;
            this.ConversionRange = conversionRange;
            this.TieredDistributions = distributions;
            this.FlattenedDistributions = flattenedDistributions;
        }

        /// <summary>
        /// To aggregate a collection of distributions
        /// </summary>
        public static IEnumerable<BundleDistribution> AggregateDistributions(IEnumerable<BundleDistribution> distributions)
        {
            var first = distributions.First();
            var aggregateStartDate = first.DistributionPeriod.StartDate;
            var onGoingDistribution = first;
            // Apply floor distribution to flattened distributions for subsequent distributions
            return distributions
                .Skip(1)
                .Select(dist => onGoingDistribution = dist.ApplyFloorDistributionToFlattenedDistributions(aggregateStartDate, onGoingDistribution.FlattenedDistributions.Last().MonthDistribution))
                .Prepend(first);
        }

        private BundleDistribution ApplyFloorDistributionToFlattenedDistributions(DateTime floorStartDate, IEnumerable<decimal?> monthDistribution)
        {
            return new BundleDistribution(
                this.DistributionPeriod,
                this.ConversionRange,
                this.TieredDistributions,
                this.FlattenedDistributions.Select(flattenedDist => flattenedDist.Aggregate(floorStartDate, monthDistribution))
            );
        }
    }
}
