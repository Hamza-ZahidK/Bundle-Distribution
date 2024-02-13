using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using ProductivMD.Server.Api.Report.Dtos;
using ProductivMD.Server.Data;
using ProductivMD.Server.Data.Models;

namespace ProductivMD.Server.Api.Report.Builders
{
    public class OrganizationProductivityBuilder
    {
        private const string ReportType = "Organization Productivity";
        private const string AdjustmentMetric = "ADJUSTMENT";
        private const string GrossMetric = "GROSS";
        private const string NetMetric = "NET";
        private const string VolumeMetric = "VOLUME";

        private static readonly CultureInfo _cultureInfo = new CultureInfo("en-us");
        private readonly OrganizationContext _organizationContext;
        private readonly ReportContext _reportContext;

        public OrganizationProductivityBuilder(OrganizationContext organizationContext, ReportContext reportContext)
        {
            this._organizationContext = organizationContext;
            this._reportContext = reportContext;

            _cultureInfo.NumberFormat.CurrencyNegativePattern = 0;
        }

        public async Task<ReportDto> BuildAsync(Guid organizationId, string metric, int endYear, int endMonth, int onlyTopAndBottomRows, bool excludeInactiveProviders)
        {
            var startDate = new DateTime(endYear, endMonth, 1).AddMonths(1).AddYears(-1);
            var endDate = new DateTime(endYear, endMonth, 1).AddMonths(1).AddDays(-1);
            var reportMonths = Enumerable.Range(0, 12).Select(e => startDate.AddMonths(e));

            var organizationName = this._organizationContext.Organization.Find(organizationId).Name;
            var providers = await this._organizationContext.Provider
                .AsNoTracking()
                .Where(x => x.OrganizationId == organizationId && x.IsActive == excludeInactiveProviders ? true : x.IsActive)
                .ToListAsync()
                .ConfigureAwait(false);

            var orgProductivity = await this._reportContext.ProviderProductivityMonthSummaries
                .FromSqlInterpolated($"rpt.RunOrganizationProductivityMonthly {organizationId}, {startDate.Year}, {startDate.Month}, {endDate.Year}, {endDate.Month}, null, {onlyTopAndBottomRows}, {excludeInactiveProviders}")
                .AsNoTracking()
                .ToListAsync()
                .ConfigureAwait(false);

            var groupedProductivity = orgProductivity.GroupBy(x => x.ProviderId).ToList();

            var applicableProviders = providers
                .Where(p => groupedProductivity.Any(g => g.Key == p.Id))
                .OrderBy(p => p.CredentialedName)
                .ToList();

            var data = applicableProviders.Select(ap => groupedProductivity.Single(g => g.Key == ap.Id).ToList());
            var metricDataAndFormat = GetMetricDataAndFormats(data, metric, reportMonths);

            return new ReportDto
            {
                Header = new
                {
                    OrganizationName = organizationName,
                    Metric = metric.ToUpperInvariant() == VolumeMetric ? "Procedure Volume" : $"{char.ToUpperInvariant(metric[0]) + metric.Substring(1)} wRVUs"
                },
                Body = new
                {
                    ProviderProductivityTable = BuildMetricSummaryTable(applicableProviders, reportMonths, metricDataAndFormat.Item1, metricDataAndFormat.Item2, metricDataAndFormat.Item3),
                },
                Meta = new
                {
                    ReportType,
                    OrganizationId = organizationId,
                    metric,
                    StartDate = startDate,
                    EndDate = endDate,
                    GeneratedAt = DateTime.UtcNow.ToString("yyyy-MM-dd H:mm:ss", _cultureInfo)
                }
            };
        }

        private static Tuple<IEnumerable<IEnumerable<decimal?>>, string, string> GetMetricDataAndFormats(
            IEnumerable<List<ProviderProductivityMonthSummary>> data,
            string metric,
            IEnumerable<DateTime> reportMonths)
        {
            switch (metric?.ToUpperInvariant())
            {
                case AdjustmentMetric:
                    return Tuple.Create(data.Select(x => reportMonths.Select(m => x.FirstOrDefault(s => s.Year == m.Year && s.Month == m.Month)?.AdjustmentWorkRvus)), "n2", "n5");
                case GrossMetric:
                    return Tuple.Create(data.Select(x => reportMonths.Select(m => x.FirstOrDefault(s => s.Year == m.Year && s.Month == m.Month)?.GrossWorkRvus)), "n2", "n5");
                case NetMetric:
                    return Tuple.Create(data.Select(x => reportMonths.Select(m => x.FirstOrDefault(s => s.Year == m.Year && s.Month == m.Month)?.NetWorkRvus)), "n2", "n5");
                case VolumeMetric:
                    return Tuple.Create(data.Select(x => reportMonths.Select(m => (decimal?)x.FirstOrDefault(s => s.Year == m.Year && s.Month == m.Month)?.Units)), "n0", "n0");
                default:
                    throw new Exception($"Productivity metric not found: {metric}");
            }
        }

        private static IEnumerable<Dictionary<string, DualFormatDto>> BuildMetricSummaryTable(
            IEnumerable<Provider> providers,
            IEnumerable<DateTime> months,
            IEnumerable<IEnumerable<decimal?>> providerMonthlyData,
            string shortFormat,
            string longFormat)
        {
            var totals = Enumerable.Range(0, months.Count()).Select(i => providerMonthlyData.Any(b => b.ElementAt(i).HasValue) ? (providerMonthlyData.Sum(b => b.ElementAt(i))) : (null));
            return providers.Zip(providerMonthlyData, (p, b) => BuildMetricSummaryRow(p, months, b, shortFormat, longFormat)).Prepend(BuildMetricSummaryRow(null, months, totals, shortFormat, longFormat));
        }

        private static Dictionary<string, DualFormatDto> BuildMetricSummaryRow(
            Provider provider, IEnumerable<DateTime> months, IEnumerable<decimal?> productivityMonth, string shortFormat, string longFormat)
        {
            var defaultItems = new KeyValuePair<string, DualFormatDto>[]
            {
                new KeyValuePair<string, DualFormatDto>("Provider", new DualFormatDto { ShortFormat = provider?.CredentialedName ?? "All Providers", LongFormat = string.Empty }),
                new KeyValuePair<string, DualFormatDto>("Total", productivityMonth.Any(b => b.HasValue)
                    ? new DualFormatDto { ShortFormat = productivityMonth.Sum(b => b ?? 0M).ToString(shortFormat, _cultureInfo), LongFormat = productivityMonth.Sum(b => b ?? 0M).ToString(longFormat, _cultureInfo) }
                    : new DualFormatDto { ShortFormat = "--", LongFormat = string.Empty })
            };

            var monthItems = months.Zip(productivityMonth, (month, value) =>
                new KeyValuePair<string, DualFormatDto>($"{month.Month}/{month.Year}",
                    new DualFormatDto { ShortFormat = value?.ToString(shortFormat, _cultureInfo) ?? "--", LongFormat = value?.ToString(longFormat, _cultureInfo) ?? string.Empty }));

            var allItems = defaultItems.Concat(monthItems);

            return new Dictionary<string, DualFormatDto>(allItems);
        }
    }
}
