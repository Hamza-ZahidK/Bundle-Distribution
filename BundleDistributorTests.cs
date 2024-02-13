using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProductivMD.Server.Compensation.Enums;

namespace ProductivMD.Server.Compensation.Tests
{
    [TestClass()]
    public class BundleDistributorTests
    {

        [TestMethod()]
        [ExpectedException(typeof(ArgumentNullException))]
        public void BundleDistributor_WithNullAccrualPeriodDecipher_ThrowsException()
        {
            //arrange
            var distributor = new BundleDistributor(null);
            //act
            //assert
            Assert.Fail();
        }

        [TestMethod()]
        [DataRow(AccrualInterval.Annual)]
        [DataRow(AccrualInterval.SemiAnnual)]
        [DataRow(AccrualInterval.Quarterly)]
        [DataRow(AccrualInterval.Monthly)]
        public void Distribute_AccrualInterval_DoesNotAffectBundleDistributionPeriod(AccrualInterval testInterval)
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 5), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 12, 31));
            var bundleWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 9, 30));
            var apd = new AccrualPeriodDecipher(reportWindow, testInterval);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal>(), new List<decimal> { 10 }, bundleWindow, false);

            //assert
            Assert.AreEqual(bundleWindow, bundleDistribution.DistributionPeriod);
        }

        [TestMethod()]
        [DataRow(AccrualInterval.Annual)]
        [DataRow(AccrualInterval.SemiAnnual)]
        [DataRow(AccrualInterval.Quarterly)]
        [DataRow(AccrualInterval.Monthly)]
        public void Distribute_CalendarYearReportWindow_HasTwelveMonthDistributionsPerTierDistribution(AccrualInterval testInterval)
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 5), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 12, 31));
            var bundleWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 9, 30));
            var apd = new AccrualPeriodDecipher(reportWindow, testInterval);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal>(), new List<decimal> { 10 }, bundleWindow, false);

            //assert
            bundleDistribution.TieredDistributions.ToList()
                .ForEach(d => Assert.IsTrue(d.TierDistributions.ToList().TrueForAll(td => td.MonthDistribution.Count() == 12)));
        }

        [TestMethod()]
        [DataRow(AccrualInterval.Annual)]
        [DataRow(AccrualInterval.SemiAnnual)]
        [DataRow(AccrualInterval.Quarterly)]
        [DataRow(AccrualInterval.Monthly)]
        public void Distribute_MidMonthYearlyReportWindow_HasThirteenMonthDistributionsPerTierDistribution(AccrualInterval testInterval)
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 5), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 15), new DateTime(2020, 1, 14));
            var bundleWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 9, 30));
            var apd = new AccrualPeriodDecipher(reportWindow, testInterval);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal>(), new List<decimal> { 10 }, bundleWindow, false);

            //assert
            bundleDistribution.TieredDistributions.ToList()
                .ForEach(d => Assert.IsTrue(d.TierDistributions.ToList().TrueForAll(td => td.MonthDistribution.Count() == 13)));
        }

        [TestMethod()]
        [DataRow(AccrualInterval.Annual, "01/01/2019", "12/31/2019")]
        [DataRow(AccrualInterval.SemiAnnual, "01/01/2019", "06/30/2019")]
        [DataRow(AccrualInterval.Quarterly, "01/01/2019", "3/31/2019")]
        [DataRow(AccrualInterval.Monthly, "01/01/2019", "1/31/2019")]
        public void Distribute_AccrualInterval_ChangesTieredDistributionAccrualPeriods(AccrualInterval testInterval, string expectedStartDate, string expectedEndDate)
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 5), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 12, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, testInterval);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal>(), new List<decimal> { 10 }, reportWindow, false);

            //assert
            var expectedDateRange = new DateRange(DateTime.Parse(expectedStartDate, CultureInfo.InvariantCulture), DateTime.Parse(expectedEndDate, CultureInfo.InvariantCulture));
            Assert.AreEqual(expectedDateRange, bundleDistribution.TieredDistributions.First().AccrualPeriod);
        }

        [TestMethod()]
        [DataRow(AccrualInterval.Annual, "01/17/2019", "01/16/2020")]
        [DataRow(AccrualInterval.SemiAnnual, "01/17/2019", "07/16/2019")]
        [DataRow(AccrualInterval.Quarterly, "01/17/2019", "4/16/2019")]
        [DataRow(AccrualInterval.Monthly, "01/17/2019", "2/16/2019")]
        public void Distribute_MidMonthStartAndEnd_HasCorrectTieredDistributionAccrualPeriods(AccrualInterval testInterval, string expectedStartDate, string expectedEndDate)
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 5), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 17), new DateTime(2020, 1, 16));
            var apd = new AccrualPeriodDecipher(reportWindow, testInterval);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal>(), new List<decimal> { 10 }, reportWindow, false);

            //assert
            var expectedDateRange = new DateRange(DateTime.Parse(expectedStartDate, CultureInfo.InvariantCulture), DateTime.Parse(expectedEndDate, CultureInfo.InvariantCulture));
            Assert.AreEqual(expectedDateRange, bundleDistribution.TieredDistributions.First().AccrualPeriod);
        }

        [TestMethod()]
        public void Distribute_WithCumulativeFlag_HasAccrualPeriodsWithSameStartDate()
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 2, 10), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 12, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, AccrualInterval.Monthly);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal>(), new List<decimal> { 10 }, reportWindow, true);

            //assert
            Assert.IsTrue(bundleDistribution.TieredDistributions.ToList().TrueForAll(t => t.AccrualPeriod.StartDate == new DateTime(2019, 1, 1)));
        }

        [TestMethod()]
        [DynamicData(nameof(ExpectedProratedTierValues), DynamicDataSourceType.Method)]
        public void Distribute_WithMultipleTiers_ProratesTierBounds(AccrualInterval interval, NumericRange[] expectedResults)
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 10), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 2, 1), new DateTime(2019, 5, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, interval);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal> { 100, 200, 300 }, new List<decimal> { 10, 20, 25, 30 }, reportWindow, false);

            //assert
            Assert.IsTrue(bundleDistribution.TieredDistributions.First().TierDistributions.Count() == 4);

            Assert.IsTrue(expectedResults
                .SequenceEqual(bundleDistribution
                    .TieredDistributions
                    .First()
                    .TierDistributions
                    .Select(d => new NumericRange(
                        d.Bounds.LowerBound.HasValue ? decimal.Round(d.Bounds.LowerBound.Value, 5) : default(decimal?),
                        d.Bounds.UpperBound.HasValue ? decimal.Round(d.Bounds.UpperBound.Value, 5) : default(decimal?)
                    ))
                ));
        }

        [TestMethod()]
        [DynamicData(nameof(ExpectedCumulativeProratedTierValues), DynamicDataSourceType.Method)]
        public void Distribute_WithCumulativeFlag_ProratesAndAccumulatesTierBounds(AccrualInterval interval, NumericRange[] expectedResults)
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 8, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, interval);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal> { 100, 200, 300 }, new List<decimal> { 10, 20, 25, 30 }, reportWindow, true);

            //assert
            Assert.IsTrue(expectedResults
                .SequenceEqual(bundleDistribution
                    .TieredDistributions
                    .Last()
                    .TierDistributions
                    .Select(d => new NumericRange(
                        d.Bounds.LowerBound.HasValue ? decimal.Round(d.Bounds.LowerBound.Value, 5) : default(decimal?),
                        d.Bounds.UpperBound.HasValue ? decimal.Round(d.Bounds.UpperBound.Value, 5) : default(decimal?)
                    ))
                ));
        }

        [TestMethod()]
        public void Distribute_WithCumulativeFlag_HasProductivityDispersedAcrossAccumulatedTierBounds()
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 10), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 2, 28));
            var apd = new AccrualPeriodDecipher(reportWindow, AccrualInterval.Monthly);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal> { 6000, 9000 }, new List<decimal> { 10, 20, 30 }, reportWindow, true);

            //assert
            var expectedFirstAccrualPeriodDistributionTotal = new decimal[] { 500, 250, 250 };
            var expectedLastAccrualPeriodDistributionTotal = new decimal[] { 1000, 0, 0 };

            // first accrual period
            Assert.IsTrue(expectedFirstAccrualPeriodDistributionTotal
                .SequenceEqual(bundleDistribution.TieredDistributions.First().TierDistributions.Select(d => decimal.Round(d.Total.Value, 5))
                ));

            // last accrual period
            Assert.IsTrue(expectedLastAccrualPeriodDistributionTotal
                .SequenceEqual(bundleDistribution.TieredDistributions.Last().TierDistributions.Select(d => decimal.Round(d.Total.Value, 5))
                ));
        }

        [TestMethod()]
        public void Distribute_FlattenedDistribution_HasConvertedProductivityAmounts()
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 10), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, AccrualInterval.Monthly);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal>(), new List<decimal> { 10 }, reportWindow, false);

            //assert
            Assert.AreEqual(10000m, bundleDistribution.FlattenedDistributions.Sum(f => f.Total));
        }

        [TestMethod()]
        public void Distribute_FlattenedDistributionForMultipleTiers_EqualsSumOfConvertedTierTotals()
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, AccrualInterval.Monthly);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal> { 2400, 7200 }, new List<decimal> { 10, 30, 50 }, reportWindow, false);
            var sumOfTierConvertedTotals = bundleDistribution.TieredDistributions.First().TierDistributions.Sum(x => x.ConvertedTotal.Value);
            var sumOfFlattenedTotals = bundleDistribution.FlattenedDistributions.First().Total.Value;

            //assert
            Assert.AreEqual(decimal.Round(sumOfTierConvertedTotals, 5), decimal.Round(sumOfFlattenedTotals, 5));
        }

        [TestMethod()]
        public void Distribute_BundleWithSingleTier_HasProductivityInOneTier()
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, AccrualInterval.Monthly);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal>(), new List<decimal> { 10 }, reportWindow, false);

            //assert
            Assert.AreEqual(3100m, bundleDistribution.TieredDistributions.First().TierDistributions.Sum(f => f.Total));
        }

        [TestMethod()]
        public void Distribute_BundleWithMultipleTiers_HasProductivityAcrossTiers()
        {
            //arrange
            var mockProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 10), 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, AccrualInterval.Monthly);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal> { 2400, 7200 }, new List<decimal> { 10, 30, 50 }, reportWindow, false);

            //assert
            // 2400 / 12 = 200, 7200 / 12 = 600
            Assert.AreEqual(200m, decimal.Round(bundleDistribution.TieredDistributions.First().TierDistributions.First().Total.Value, 5));
            Assert.AreEqual(400m, decimal.Round(bundleDistribution.TieredDistributions.First().TierDistributions.Skip(1).First().Total.Value, 5));
            Assert.AreEqual(400m, decimal.Round(bundleDistribution.TieredDistributions.First().TierDistributions.Last().Total.Value, 5));
        }

        [TestMethod()]
        public void Distribute_ProductivityOutsideReportWindow_IsNotIncluded()
        {
            //arrange
            var mockJanuaryProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31), 100m);
            var mockMarchProductivity = MockDailyProductivity(new DateTime(2019, 3, 1), new DateTime(2019, 3, 31), 100m);
            var februaryReportWindow = new DateRange(new DateTime(2019, 2, 1), new DateTime(2019, 2, 28));
            var apd = new AccrualPeriodDecipher(februaryReportWindow, AccrualInterval.Monthly);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockJanuaryProductivity.Concat(mockMarchProductivity), new List<decimal>(), new List<decimal> { 10 }, februaryReportWindow, false);

            //assert
            Assert.IsTrue(bundleDistribution.FlattenedDistributions.ToList().TrueForAll(f => f.Total == default));
        }

        [TestMethod()]
        [DataRow("12/10/2018", "01/01/2019", "100")]
        [DataRow("12/10/2018", "01/14/2019", "1400")]
        [DataRow("12/20/2018", "02/14/2019", "3100")]
        [DataRow("01/10/2019", "02/14/2019", "2200")]
        [DataRow("01/31/2019", "02/14/2019", "100")]
        public void Distribute_ProductivityOverlappingReportWindow_HasCorrectProductivityTotal(string testProductivityStartDate, string testProductivityEndDate, string expectedResult)
        {
            //arrange
            var startDate = DateTime.Parse(testProductivityStartDate, CultureInfo.InvariantCulture);
            var endDate = DateTime.Parse(testProductivityEndDate, CultureInfo.InvariantCulture);

            var mockProductivity = MockDailyProductivity(startDate, endDate, 100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, AccrualInterval.Monthly);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockProductivity, new List<decimal>(), new List<decimal> { 10 }, reportWindow, false);

            //assert
            var sumTotal = bundleDistribution.TieredDistributions.First().TierDistributions.Sum(t => t.Total);
            Assert.AreEqual(decimal.Parse(expectedResult, CultureInfo.InvariantCulture), sumTotal.HasValue ? decimal.Round(sumTotal.Value, 5) : default(decimal?));
        }

        [TestMethod()]
        public void Distribute_NegativeProductivity_HasNegativeDistributionAmounts()
        {
            //arrange
            var mockNegativeProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 5), -100m);
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, AccrualInterval.Annual);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockNegativeProductivity, new List<decimal>(), new List<decimal> { 10 }, reportWindow, false);

            //assert
            Assert.AreEqual(-5000m, bundleDistribution.FlattenedDistributions.Sum(f => f.Total));
            Assert.AreEqual(-500m, bundleDistribution.TieredDistributions.First().TierDistributions.Sum(f => f.Total));
        }

        [TestMethod()]
        public void Distribute_NegativeProductivity_ReducesPositiveProductivityTotal()
        {
            //arrange
            var mockNegativeProductivity = MockDailyProductivity(new DateTime(2019, 1, 1), new DateTime(2019, 1, 3), -100m); //-300
            var mockPositiveProductivity = MockDailyProductivity(new DateTime(2019, 1, 10), new DateTime(2019, 1, 19), 100m); //1000
            var reportWindow = new DateRange(new DateTime(2019, 1, 1), new DateTime(2019, 1, 31));
            var apd = new AccrualPeriodDecipher(reportWindow, AccrualInterval.Annual);
            var distributor = new BundleDistributor(apd);

            //act
            var bundleDistribution = distributor.Distribute(mockNegativeProductivity.Concat(mockPositiveProductivity), new List<decimal>(), new List<decimal> { 10 }, reportWindow, false);

            //assert
            Assert.AreEqual(7000m, bundleDistribution.FlattenedDistributions.Sum(f => f.Total));
            Assert.AreEqual(700m, bundleDistribution.TieredDistributions.First().TierDistributions.Sum(f => f.Total));
        }

        private static IEnumerable<KeyValuePair<DateTime, decimal?>> MockDailyProductivity(DateTime startDate, DateTime endDate, decimal productivity) =>
            Enumerable.Range(0, endDate.Subtract(startDate).Days + 1).Select(x => new KeyValuePair<DateTime, decimal?>(startDate.AddDays(x), productivity));

        private static IEnumerable<object[]> ExpectedProratedTierValues()
        {
            return new[]
            {
                new object[]
                {
                    AccrualInterval.Annual,
                    new NumericRange[] { new NumericRange(null, 32.87671m), new NumericRange(32.87671m, 65.75342m), new NumericRange(65.75342m, 98.63014m), new NumericRange(98.63014m, null) }
                },
                new object[]
                {
                    AccrualInterval.SemiAnnual,
                    new NumericRange[] { new NumericRange(null, 33.14917m), new NumericRange(33.14917m, 66.29834m), new NumericRange(66.29834m, 99.44751m), new NumericRange(99.44751m, null) }
                },
                new object[]
                {
                    AccrualInterval.Quarterly,
                    new NumericRange[] { new NumericRange(null, 25), new NumericRange(25, 50), new NumericRange(50, 75), new NumericRange(75, null) }
                },
                new object[]
                {
                    AccrualInterval.Monthly,
                    new NumericRange[] { new NumericRange(null, 8.33333m), new NumericRange(8.33333m, 16.66667m), new NumericRange(16.66667m, 25), new NumericRange(25, null) }
                }
            };
        }

        private static IEnumerable<object[]> ExpectedCumulativeProratedTierValues()
        {
            return new[]
            {
                new object[]
                {
                    AccrualInterval.Annual,
                    new NumericRange[] { new NumericRange(null, 66.57534m), new NumericRange(66.57534m, 133.15068m), new NumericRange(133.15068m, 199.72603m), new NumericRange(199.72603m, null) }
                },
                new object[]
                {
                    AccrualInterval.SemiAnnual,
                    new NumericRange[] { new NumericRange(null, 66.84783m), new NumericRange(66.84783m, 133.69565m), new NumericRange(133.69565m, 200.54348m), new NumericRange(200.54348m, null) }
                },
                new object[]
                {
                    AccrualInterval.Quarterly,
                    new NumericRange[] { new NumericRange(null, 66.84783m), new NumericRange(66.84783m, 133.69565m), new NumericRange(133.69565m, 200.54348m), new NumericRange(200.54348m, null) }
                },
                new object[]
                {
                    AccrualInterval.Monthly,
                    new NumericRange[] { new NumericRange(null, 66.66667m), new NumericRange(66.66667m, 133.33333m), new NumericRange(133.33333m, 200), new NumericRange(200, null) }
                }
            };
        }
    }
}
