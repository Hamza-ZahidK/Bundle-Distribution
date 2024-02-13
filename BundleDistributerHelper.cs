using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ProductivMD.Server.Api.Common.Dtos;
using ProductivMD.Server.Compensation;
using ProductivMD.Server.Compensation.Enums;

namespace ProductivMD.Server.Api.Common.Helpers
{
    public class BundleDistributerHelper
    {
        public BundleDistributerHelper()
        {
        }

        /// <summary>
        ///  Distribute bundles to calculate the productivity
        /// </summary>
        public IEnumerable<BundleDistribution> Distribute(IEnumerable<KeyValuePair<DateTime, decimal?>> values, List<TierBundleDto> tierBundleDtos, AccrualPeriodDecipher decipher)
        {
            var bundleDistributor = new BundleDistributor(decipher);

            return tierBundleDtos.Select(b =>
                  bundleDistributor.Distribute(
                          values,

                          b.Tiers
                          .OrderBy(t => t.LowerBound ?? decimal.MinValue)
                          .Skip(1)
                          .Select(t => t.LowerBound ?? 0),

                          b.Tiers
                          .OrderBy(t => t.LowerBound ?? decimal.MinValue)
                          .Select(t => t.ConversionFactor),
                          new DateRange(b.EffectiveFrom, b.EffectiveTo.Value),
                          false
                      )
                  );
        }
    }
}
