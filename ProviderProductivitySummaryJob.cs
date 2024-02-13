using System.Data;
using System.Diagnostics.Metrics;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using ProductivMD.Server.Api.Common.Dtos;
using ProductivMD.Server.Compensation;
using ProductivMD.Server.Compensation.Enums;
using ProductivMD.Server.Data;
using ProductivMD.Server.Data.Models;

namespace ProductivMD.Server.Api.Common.Helpers
{
    public class ProviderProductivitySummaryJob
    {
        private readonly EvaluationContext _evaluationContext;
        private readonly CompensationContext _compensationContext;

        public ProviderProductivitySummaryJob(EvaluationContext evaluationContext, CompensationContext compensationContext)
        {
            this._evaluationContext = evaluationContext;
            this._compensationContext = compensationContext;
        }

        /// <summary>
        /// ProviderProductivitySummaryJob: Monthly productivity calculation for providers.
        /// This job calculates and saves Work RVUs, units, and productivity per month in the ProviderProductivitySummary Table.
        /// For the first run, it calculates productivity against all records in eval.ProcedureProductivity for providers.
        /// For subsequent runs, it considers records in ProcedureProductivity added or modified after the last update in ProviderProductivitySummary.
        /// If there are changes in provider tier bundles or new bundles are added, productivity is recalculated for affected providers.
        /// </summary>
        public async Task<string> RunProductivitySummaryForProviders()
        {
            List<TierBundleDto> tierBundles = new List<TierBundleDto>();

            List<ProvidersProcedureProductivity> providersProductivityMonthly = new List<ProvidersProcedureProductivity>();
            List<ProvidersProcedureProductivity> providersProductivityWithDateRange = new List<ProvidersProcedureProductivity>();
            List<ProviderWithDateRangeDto> providersWithMultipleTierBundlesInMonth = new List<ProviderWithDateRangeDto>();

            if (this._evaluationContext.ProviderProductivitySummary.Any()) //This check will be used if there is no record exists in the table, as for first time there will be no record
            {
                var lastUpdatedDate = this._evaluationContext.ProviderProductivitySummary.Max(pps => pps.LastUpdated);

                var providerIdsWithNewOrUpdatedTierBundles = await GetProvidersWithNewOrUpdatedTierBundles(lastUpdatedDate).ConfigureAwait(false);

                List<ProviderForNewOrUpdatedWRvus> providersForNewOrUpdatedWRvus = new List<ProviderForNewOrUpdatedWRvus>();

                providersForNewOrUpdatedWRvus = await GetNewOrUpdatedWRvusProviders(lastUpdatedDate).ConfigureAwait(false);

                List<ProviderWithMonthYearDto> providersWithMonthYear = new List<ProviderWithMonthYearDto>();

                if (providerIdsWithNewOrUpdatedTierBundles.Any())
                {
                    providersForNewOrUpdatedWRvus = providersForNewOrUpdatedWRvus.Where(p => !providerIdsWithNewOrUpdatedTierBundles.Contains(p.Id)).ToList();

                    foreach (var providerId in providerIdsWithNewOrUpdatedTierBundles)
                    {
                        providersWithMonthYear.Add(new ProviderWithMonthYearDto
                        {
                            Id = providerId,
                            IsForAllMonths = true
                        });
                    }
                }

                var providerIdsForTierBundles = providersForNewOrUpdatedWRvus.Select(p => p.Id).ToList();
                providerIdsForTierBundles.AddRange(providerIdsWithNewOrUpdatedTierBundles);

                // Adding providerYearMonths that have changed and were not already present in providersWithMonthYear
                providersWithMonthYear.AddRange(
                    providersForNewOrUpdatedWRvus
                    .Where(p =>
                    !providersWithMonthYear.Any(existing =>
                    existing.Id == p.Id &&
                    (existing.IsForAllMonths || (existing.Month == p.Month && existing.Year == p.Year))
                    )
                    )
                    .Select(p => new ProviderWithMonthYearDto
                    {
                        Id = p.Id,
                        Month = p.Month,
                        Year = p.Year,
                        IsForAllMonths = false
                    })
                    .ToList());


                tierBundles = await GetTiersBundles(providerIdsForTierBundles.Distinct().ToList(), false).ConfigureAwait(false);

                tierBundles = GetFilteredTierBundles(tierBundles, providersForNewOrUpdatedWRvus, providerIdsWithNewOrUpdatedTierBundles);

                providersWithMultipleTierBundlesInMonth = GetProvidersWithMultipleTierBundlesInMonth(tierBundles, providersForNewOrUpdatedWRvus, providerIdsWithNewOrUpdatedTierBundles, false);

                providersWithMonthYear = providersWithMonthYear.Where(p =>
                                !providersWithMultipleTierBundlesInMonth.Any(pwmtb =>
                                pwmtb.Id == p.Id &&
                                pwmtb.EndDate.Year == p.Year &&
                                pwmtb.EndDate.Month == p.Month &&
                                pwmtb.StartDate.Year == p.Year &&
                                pwmtb.StartDate.Month == p.Month
                                )).ToList();

                providersProductivityMonthly = GetProvidersProcedureProductivitiesMonthly(providersWithMonthYear, false);



            }
            else
            {
                providersProductivityMonthly = GetProvidersProcedureProductivitiesMonthly(new List<ProviderWithMonthYearDto>(), true);

                tierBundles = await GetTiersBundles(new List<Guid>(), true).ConfigureAwait(false);

                providersWithMultipleTierBundlesInMonth = GetProvidersWithMultipleTierBundlesInMonth(tierBundles, null, null);

                providersProductivityMonthly = GetFilteredProvidersProcedureProductivities(providersProductivityMonthly, providersWithMultipleTierBundlesInMonth);

            }

            providersProductivityWithDateRange = GetProvidersProcedureProductivitiesForDateRange(providersWithMultipleTierBundlesInMonth);
            providersProductivityMonthly.AddRange(providersProductivityWithDateRange);

            SetProvidersTotalProductivity(tierBundles, providersProductivityMonthly);

            return await SaveOrUpdateProvidersMonthlyProductivitySummary(providersProductivityMonthly).ConfigureAwait(false);
        }


        #region Private Mehtods

        private List<ProvidersProcedureProductivity> GetProvidersProcedureProductivitiesMonthly(List<ProviderWithMonthYearDto> providersWithMonthYear, bool productivityForAllProviders)
        {
            List<ProvidersProcedureProductivity> result = new List<ProvidersProcedureProductivity>();

            if (!productivityForAllProviders && !providersWithMonthYear.Any())
            {
                return result;
            }

            DataTable providersDataTable = new DataTable();
            providersDataTable.Columns.Add("Id", typeof(Guid));
            providersDataTable.Columns.Add("Month", typeof(int));
            providersDataTable.Columns.Add("Year", typeof(int));
            providersDataTable.Columns.Add("IsForAllMonths", typeof(bool));

            if (!productivityForAllProviders)
            {
                foreach (var provider in providersWithMonthYear)
                {
                    providersDataTable.Rows.Add(provider.Id, provider.Month, provider.Year, provider.IsForAllMonths);
                }
            }

            int connectionTimeOutSeconds = 1800;
            var cmd = this._evaluationContext.Database.GetDbConnection().CreateCommand();
            cmd.CommandText = $"eval.GetProvidersProductivityMonthly";
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = connectionTimeOutSeconds;

            cmd.Parameters.Add(new SqlParameter("@providers", SqlDbType.Structured) { TypeName = "dbo.ProvidersWithMonthYear", Value = providersDataTable });
            cmd.Parameters.Add(new SqlParameter("@productivityForAllProviders", productivityForAllProviders));

            if (cmd.Connection?.State != System.Data.ConnectionState.Open)
            {
                cmd.Connection?.Open();
            }

            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    ProvidersProcedureProductivity providerWorkRvus = new ProvidersProcedureProductivity();
                    providerWorkRvus.Id = reader.GetGuid(reader.GetOrdinal("ProviderId"));
                    providerWorkRvus.NetWorkRvus = reader.GetDecimal(reader.GetOrdinal("NetWorkRvus"));
                    providerWorkRvus.GrossWorkRvus = reader.GetDecimal(reader.GetOrdinal("GrossWorkRvus"));
                    providerWorkRvus.Units = reader.GetInt32(reader.GetOrdinal("Units"));
                    int month = reader.GetInt32(reader.GetOrdinal("Month"));
                    int year = reader.GetInt32(reader.GetOrdinal("Year"));
                    providerWorkRvus.Month = month;
                    providerWorkRvus.Year = year;
                    // Set StartDate to the beginning of the month.
                    providerWorkRvus.StartDate = new DateTime(year, month, 1);
                    // Set EndDate to the end of the month.
                    providerWorkRvus.EndDate = new DateTime(year, month, DateTime.DaysInMonth(year, month));
                    result.Add(providerWorkRvus);
                }
            }

            return result;
        }

        private List<ProvidersProcedureProductivity> GetProvidersProcedureProductivitiesForDateRange(List<ProviderWithDateRangeDto> providersWithDateRange)
        {
            if (providersWithDateRange is null || !providersWithDateRange.Any())
            {
                return new List<ProvidersProcedureProductivity>();
            }

            List<ProvidersProcedureProductivity> result = new List<ProvidersProcedureProductivity>();
            DataTable providersDataTable = new DataTable();
            providersDataTable.Columns.Add("Id", typeof(Guid));
            providersDataTable.Columns.Add("StartDate", typeof(DateTime));
            providersDataTable.Columns.Add("EndDate", typeof(DateTime));

            foreach (var provider in providersWithDateRange)
            {
                providersDataTable.Rows.Add(provider.Id, provider.StartDate, provider.EndDate);
            }


            int connectionTimeOutSeconds = 1800;
            var cmd = this._evaluationContext.Database.GetDbConnection().CreateCommand();
            cmd.CommandText = $"eval.GetProvidersProductivityForDateRange";
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            cmd.CommandTimeout = connectionTimeOutSeconds;

            cmd.Parameters.Add(new SqlParameter("@providers", SqlDbType.Structured) { TypeName = "dbo.ProviderWithDateRange", Value = providersDataTable });

            if (cmd.Connection?.State != System.Data.ConnectionState.Open)
            {
                cmd.Connection?.Open();
            }

            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    ProvidersProcedureProductivity providerWorkRvus = new ProvidersProcedureProductivity();
                    providerWorkRvus.Id = reader.GetGuid(reader.GetOrdinal("ProviderId"));
                    providerWorkRvus.NetWorkRvus = reader.GetDecimal(reader.GetOrdinal("NetWorkRvus"));
                    providerWorkRvus.GrossWorkRvus = reader.GetDecimal(reader.GetOrdinal("GrossWorkRvus"));
                    providerWorkRvus.Units = reader.GetInt32(reader.GetOrdinal("Units"));
                    var startDate = reader.GetDateTime(reader.GetOrdinal("StartDate"));
                    providerWorkRvus.StartDate = startDate;
                    providerWorkRvus.EndDate = reader.GetDateTime(reader.GetOrdinal("EndDate"));
                    providerWorkRvus.Month = startDate.Month;
                    providerWorkRvus.Year = startDate.Year;
                    result.Add(providerWorkRvus);
                }
            }
            return result;
        }

        private List<ProvidersProcedureProductivity> GetFilteredProvidersProcedureProductivities(List<ProvidersProcedureProductivity> providersProductivityMonthly, List<ProviderWithDateRangeDto> providersWithMultipleTierBundlesInMonth)
        {
            return providersProductivityMonthly.Where(ppm => !providersWithMultipleTierBundlesInMonth.Any(pmr => pmr.Id == ppm.Id && pmr.StartDate.Month == ppm.Month && pmr.StartDate.Year == ppm.Year && pmr.EndDate.Month == ppm.Month && pmr.EndDate.Year == ppm.Year)).ToList();
        }

        private List<ProviderWithDateRangeDto> GetProvidersWithMultipleTierBundlesInMonth(List<TierBundleDto> tierBundles, List<ProviderForNewOrUpdatedWRvus> providersForNewOrUpdatedWRvus, List<Guid> providerIdsWithNewOrUpdatedTierBundles, bool forAllProviders = true)
        {
            List<ProviderWithDateRangeDto> providersWithMonths = GetProvidersWithMonths(tierBundles);

            if (!forAllProviders)
            {
                providersWithMonths = GetFilteredProvidersMonthRecord(providersWithMonths, providersForNewOrUpdatedWRvus, providerIdsWithNewOrUpdatedTierBundles);
            }

            var groupedProviders = providersWithMonths
           .GroupBy(p => new { Id = p.Id, Month = p.StartDate.Month, Year = p.StartDate.Year })
           .ToList();

            var result = groupedProviders
                .Where(group => group.ToList().Count() > 1)
                .SelectMany(group => group)
                .ToList();

            return result;
        }

        private async Task<string> SaveOrUpdateProvidersMonthlyProductivitySummary(List<ProvidersProcedureProductivity> providersProductivities)
        {
            if (!providersProductivities.Any())
            {
                return "No Record To Add Or Update";
            }

            List<ProviderProductivitySummary> existingRecords = await GetExistingProviderProductivitySummaries(providersProductivities).ConfigureAwait(false);

            var currentDateTime = DateTime.UtcNow;
            //update the existing records
            if (existingRecords.Any())
            {
                List<ProvidersProcedureProductivity> recordsToUpdate = new List<ProvidersProcedureProductivity>();

                foreach (var er in existingRecords)
                {
                    var recordToUpdate = providersProductivities.Where(pp =>
                        pp.Id == er.ProviderId &&
                        pp.Month == er.Month &&
                        pp.Year == er.Year).ToList();

                    if (recordToUpdate != null)
                    {
                        // Store the record to be excluded/updated
                        recordsToUpdate.AddRange(recordToUpdate);

                        // Update the existing record
                        er.TotalProductivity = recordToUpdate.Sum(r => r.TotalProductivity);
                        er.NetWorkRvus = recordToUpdate.Sum(r => r.NetWorkRvus);
                        er.GrossWorkRvus = recordToUpdate.Sum(r => r.GrossWorkRvus);
                        er.Units = recordToUpdate.Sum(r => r.Units);
                        er.LastUpdated = currentDateTime;
                    }
                }

                // Remove the records that have been updated from providersProductivities
                providersProductivities.RemoveAll(pp => recordsToUpdate.Any(r =>
                    r.Id == pp.Id &&
                    r.Month == pp.Month &&
                    r.Year == pp.Year));
            }

            ////if any new records to be add
            if (providersProductivities.Any())
            {
                List<ProviderProductivitySummary> providerProductivitySummaries = new List<ProviderProductivitySummary>();

                var providersProductivitiesGroup = providersProductivities
                    .GroupBy(pp => new { pp.Id, Month = pp.Month, Year = pp.Year })
                    .ToList();



                foreach (var providerProductivities in providersProductivitiesGroup)
                {
                    providerProductivitySummaries.Add(new ProviderProductivitySummary
                    {
                        ProviderId = providerProductivities.Key.Id,
                        Month = (int)providerProductivities.Key.Month,
                        Year = (int)providerProductivities.Key.Year,
                        GrossWorkRvus = providerProductivities.Sum(pp => pp.GrossWorkRvus),
                        NetWorkRvus = providerProductivities.Sum(pp => pp.NetWorkRvus),
                        Units = providerProductivities.Sum(pp => pp.Units),
                        TotalProductivity = providerProductivities.Sum(pp => pp.TotalProductivity),
                        LastUpdated = currentDateTime
                    });
                }

                await this._evaluationContext.ProviderProductivitySummary.AddRangeAsync(providerProductivitySummaries);
            }

            await this._evaluationContext.SaveChangesAsync();
            return "Data Added/Updated Successfully";
        }

        private async Task<List<ProviderProductivitySummary>> GetExistingProviderProductivitySummaries(List<ProvidersProcedureProductivity> providersProductivities)
        {
            List<ProviderProductivitySummary> existingRecords = await this._evaluationContext.ProviderProductivitySummary
                        .Where(pps => providersProductivities.Select(pp => pp.Id).Distinct().ToList().Contains(pps.ProviderId))
                        .ToListAsync();

            existingRecords = existingRecords
                .Where(pps => providersProductivities.Any(pp =>
                pp.Id == pps.ProviderId &&
                pp.Month == pps.Month &&
                pp.Year == pps.Year
                ))
                .ToList();

            return existingRecords;
        }

        private async Task<List<TierBundleDto>> GetTiersBundles(List<Guid> providerIds, bool getTierBundlesForAllProviders)
        {
            return await this._compensationContext.TierBundles
                .AsNoTracking()
                .Where(tb => tb.DeprecatedDate == null && (getTierBundlesForAllProviders || (providerIds.Contains(tb.ProviderId))))
                .Include(p => p.Tiers)
                .Select(TierBundleDto.Projection)
                .ToListAsync()
                .ConfigureAwait(false);
        }

        private async Task<List<ProviderForNewOrUpdatedWRvus>> GetNewOrUpdatedWRvusProviders(DateTime lastUpdatedDate)
        {
            var sqlQuery = "SELECT DISTINCT pp.ProviderId AS Id, MONTH(pp.PostDate) AS Month, YEAR(pp.PostDate) AS Year " +
           "FROM eval.ProcedureProductivity AS pp " +
           "WHERE IsAncillary = 0 AND ((pp.DeprecatedDate IS NOT NULL AND pp.DeprecatedDate >= @lastUpdatedDate) " +
           "OR pp.EffectiveDate >= @lastUpdatedDate)";

            var originalTimeout = this._evaluationContext.Database.GetCommandTimeout();
            this._evaluationContext.Database.SetCommandTimeout(1800);

            var parameters = new SqlParameter("@lastUpdatedDate", lastUpdatedDate);

            var result = await this._evaluationContext.ProviderForNewOrUpdatedWRvus
                .FromSqlRaw(sqlQuery, parameters)
                .Select(x => new ProviderForNewOrUpdatedWRvus { Id = x.Id, Month = x.Month, Year = x.Year })
                .Distinct()
                .ToListAsync()
                .ConfigureAwait(false);
            this._evaluationContext.Database.SetCommandTimeout(originalTimeout);

            return result;
        }

        private List<ProviderWithDateRangeDto> GetProvidersWithMonths(List<TierBundleDto> tierBundles)
        {
            List<ProviderWithDateRangeDto> result = new List<ProviderWithDateRangeDto>();
            var providerTierBundles = tierBundles.GroupBy(tb => tb.ProviderId).ToList();
            foreach (var providerTierBundle in providerTierBundles)
            {
                result.AddRange(GenerateMonthRecords(providerTierBundle.ToList()));
            }
            return result;
        }

        public static List<ProviderWithDateRangeDto> GenerateMonthRecords(List<TierBundleDto> tierBundles)
        {
            List<ProviderWithDateRangeDto> monthRecords = new List<ProviderWithDateRangeDto>();

            tierBundles.Sort((a, b) => a.EffectiveFrom.CompareTo(b.EffectiveFrom));

            for (int i = 0; i < tierBundles.Count; i++)
            {
                TierBundleDto tierBundle = tierBundles[i];
                DateTime startDate = tierBundle.EffectiveFrom.Date;
                DateTime endDate;

                if (tierBundle.EffectiveTo.HasValue)
                {
                    endDate = tierBundle.EffectiveTo.Value.Date;
                }
                else
                {
                    // If EffectiveTo is null, use the start date of the next record as the end date
                    endDate = (i < tierBundles.Count - 1) ? tierBundles[i + 1].EffectiveFrom.Date.AddDays(-1) : DateTime.UtcNow.Date;
                }

                // Iterate over each month
                while (startDate < endDate)
                {
                    DateTime endOfMonth = new DateTime(startDate.Year, startDate.Month, DateTime.DaysInMonth(startDate.Year, startDate.Month));

                    // If the end date is within the same month, use it; otherwise, use the end of the month
                    DateTime recordEndDate = endDate <= endOfMonth ? endDate : endOfMonth;

                    monthRecords.Add(new ProviderWithDateRangeDto
                    {
                        Id = tierBundle.ProviderId,
                        StartDate = startDate,
                        EndDate = recordEndDate
                    });

                    // Move to the next month
                    startDate = endOfMonth.AddDays(1);
                }
            }

            return monthRecords;
        }

        private async Task<List<Guid>> GetProvidersWithNewOrUpdatedTierBundles(DateTime lastUpdatedDate)
        {
            return await this._compensationContext.TierBundles.Where(tb =>
            (tb.DeprecatedDate != null && tb.DeprecatedDate >= lastUpdatedDate) || tb.CreatedDate >= lastUpdatedDate).Select(tb => tb.ProviderId).Distinct().ToListAsync().ConfigureAwait(false);
        }

        private void SetProvidersTotalProductivity(List<TierBundleDto> tierBundles, List<ProvidersProcedureProductivity> providersProductivities)
        {
            var providersWithTierBundles = tierBundles.Select(tb => tb.ProviderId).ToList();

            var productivityForProvidersWithTierBundle = providersProductivities.Where(ppm => providersWithTierBundles.Contains(ppm.Id)).ToList();

            foreach (var providerProductivity in productivityForProvidersWithTierBundle)
            {
                /*
                 * This section of the code calculates and sets the productivity of a provider with Tier Bundles.
                 * Productivity is determined using tier values within the TierBundle date ranges and model date ranges 
                 * (providerProductivity.StartDate and providerProductivity.EndDate).
                 */


                // Filter Tier Bundles for the specific provider and within the providerProductivity date range
                var bundles = tierBundles.Where(tb => tb.ProviderId == providerProductivity.Id && tb.EffectiveFrom <= providerProductivity.StartDate && (tb.EffectiveTo == null || tb.EffectiveTo >= providerProductivity.EndDate)).ToList();

                // Remove Tier Bundles without tiers with non-null lower bounds
                bundles = bundles != null ? bundles.Where(b => b.Tiers.Any(t => t.LowerBound != null)).ToList() : new List<TierBundleDto>();

                var providerProductivities = new List<ProvidersProcedureProductivity> { providerProductivity };

                var modifiedBundles = new List<TierBundleDto>();

                // Modify Tier Bundles to align with providerProductivity date range
                bundles.ForEach(b =>
                {
                    var modifiedBundle = new TierBundleDto
                    {
                        ProviderId = b.ProviderId,
                        EffectiveFrom = (b.EffectiveFrom < providerProductivity.StartDate) ? providerProductivity.StartDate : b.EffectiveFrom,
                        EffectiveTo = (b.EffectiveTo == null || b.EffectiveTo > providerProductivity.EndDate) ? providerProductivity.EndDate : b.EffectiveTo,
                        Tiers = b.Tiers
                    };

                    modifiedBundles.Add(modifiedBundle);
                });

                AccrualInterval accrualInterval = AccrualInterval.Annual;
                var reportWindow = new DateRange(providerProductivity.StartDate, providerProductivity.EndDate);

                var decipher = new AccrualPeriodDecipher(reportWindow, accrualInterval);
                BundleDistributerHelper bundleDistributerHelper = new BundleDistributerHelper();
                // Calculate bundle distributions
                var bundleDistributions = bundleDistributerHelper.Distribute(providerProductivities.Select(pp =>
                          new KeyValuePair<DateTime, decimal?>(pp.StartDate, pp.NetWorkRvus)
                          ), modifiedBundles, decipher);

                // Update providerProductivity TotalProductivity based on calculated bundle distributions
                foreach (var bundleDistribution in bundleDistributions)
                {
                    providerProductivity.TotalProductivity += (decimal)bundleDistribution.FlattenedDistributions.Sum(fs => fs.Total);
                }
            }
        }

        private List<ProviderWithDateRangeDto> GetFilteredProvidersMonthRecord(List<ProviderWithDateRangeDto> providersWithMonths, List<ProviderForNewOrUpdatedWRvus> providersForNewOrUpdatedWRvus, List<Guid> providerIdsWithNewOrUpdatedTierBundles)
        {
            return providersWithMonths.Where(pwm =>
                    (providersForNewOrUpdatedWRvus.Any(p =>
                    p.Id == pwm.Id &&
                    (p.Year == pwm.StartDate.Year && p.Month == pwm.StartDate.Month) &&
                    (pwm.EndDate.Year == p.Year && pwm.EndDate.Month == p.Month)
                    )
                    || providerIdsWithNewOrUpdatedTierBundles.Contains(pwm.Id))
                    ).ToList();
        }

        private List<TierBundleDto> GetFilteredTierBundles(List<TierBundleDto> tierBundles, List<ProviderForNewOrUpdatedWRvus> providersForNewOrUpdatedWRvus, List<Guid> providerIdsWithNewOrUpdatedTierBundles)
        {
            return tierBundles
                .Where(tb =>
                providersForNewOrUpdatedWRvus.Any(p =>
                p.Id == tb.ProviderId &&
                (p.Year > tb.EffectiveFrom.Year ||
                (p.Year == tb.EffectiveFrom.Year && p.Month >= tb.EffectiveFrom.Month))
                &&
                (tb.EffectiveTo == null ||
                tb.EffectiveTo.Value.Year > p.Year ||
                (tb.EffectiveTo.Value.Year == p.Year && tb.EffectiveTo.Value.Month >= p.Month)
                )) ||
                providerIdsWithNewOrUpdatedTierBundles.Contains(tb.ProviderId)
                ).ToList();
        }

        #endregion

    }

}
