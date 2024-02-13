using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using ProductivMD.Server.Authorization;
using ProductivMD.Server.Api.Report.Builders;
using ProductivMD.Server.Compensation.Enums;
using ProductivMD.Server.Data;
using Microsoft.EntityFrameworkCore;
using System.Security.Claims;
using TimeAndEffort.DataAccess;
using ProductivMD.Server.Data.Enums;
using System.Collections.Generic;
using ProductivMD.Server.Api.Common;
using ProductivMD.Server.Api.Common.Helpers;
using ProductivMD.Server.Api.Report.Dtos;
using ProductivMD.Server.Api.Common.Dtos;
using System.Data;

namespace ProductivMD.Server.Api.Report.Controllers
{
    [ApiController]
    public class ReportsController : ControllerBase
    {
        private readonly ReportContext _reportContext;
        private readonly OrganizationContext _organizationContext;
        private readonly CompensationContext _compensationContext;
        private readonly AuthorizationContext _authorizationContext;
        private readonly TimeAndEffortDbContext _timeAndEffortDbContext;
        private readonly UserRoleValidationHelper _userRoleValidationHelper;
        private readonly HealthSystemAndRegionHelper _healthSystemAndRegionHelper;
        private readonly EvaluationContext _evaluationContext;

        public ReportsController(ReportContext reportContext, OrganizationContext organizationContext, CompensationContext compensationContext, AuthorizationContext authorizationContext, TimeAndEffortDbContext timeAndEffortDbContext, HealthSystemAndRegionHelper healthSystemAndRegionHelper, EvaluationContext evaluationContext)
        {
            this._reportContext = reportContext;
            this._organizationContext = organizationContext;
            this._compensationContext = compensationContext;
            this._authorizationContext = authorizationContext;
            this._timeAndEffortDbContext = timeAndEffortDbContext;
            this._userRoleValidationHelper = new UserRoleValidationHelper();
            this._healthSystemAndRegionHelper = healthSystemAndRegionHelper;
            this._evaluationContext = evaluationContext;
        }

        #region Organization Reports
        [HttpGet]
        [Route("/proceduregrossnetdetail")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_FULL_ORGANIZATION_USERS)]
        public async Task<IActionResult> GetProcedureGrossNetDetail(
            [BindRequired, FromQuery] Guid providerId,
            [BindRequired, FromQuery] int year,
            [BindRequired, FromQuery] int month,
            [Bind, FromQuery] bool byServiceDate = false
        )
        {
            if (!this._organizationContext.Provider.Any(x => x.Id == providerId))
                return this.NotFound();

            var builder = new ProcedureGrossNetDetailBuilder(this._organizationContext, this._reportContext);
            return this.Ok(await builder.BuildAsync(providerId, year, month, byServiceDate).ConfigureAwait(false));
        }

        [HttpGet]
        [Route("/pmsvariancedetail")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_FULL_ORGANIZATION_USERS)]
        public async Task<IActionResult> GetPmsVarianceDetail(
            [BindRequired, FromQuery] Guid providerId,
            [BindRequired, FromQuery] int year,
            [BindRequired, FromQuery] int month,
            [Bind, FromQuery] bool byServiceDate = false
        )
        {
            if (!this._organizationContext.Provider.Any(x => x.Id == providerId))
                return this.NotFound();

            var builder = new PmsVarianceDetailBuilder(this._organizationContext, this._reportContext);
            return this.Ok(await builder.BuildAsync(providerId, year, month, byServiceDate).ConfigureAwait(false));
        }

        [HttpGet]
        [Route("/organizationproductivity")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_FULL_ORGANIZATION_USERS)]
        public async Task<IActionResult> GetOrganizationProductivity(
            [BindRequired, FromQuery] Guid organizationId,
            [BindRequired, FromQuery] string metric,
            [BindRequired, FromQuery] int month,
            [BindRequired, FromQuery] int year,
            [Bind, FromQuery] int onlyTopAndBottomRows = 0,
            [Bind, FromQuery] bool excludeInactiveProviders = true
        )
        {
            if (!this._organizationContext.Organization.Any(x => x.Id == organizationId))
                return this.NotFound();

            var builder = new OrganizationProductivityBuilder(this._organizationContext, this._reportContext);
            return this.Ok(await builder.BuildAsync(organizationId, metric, year, month, onlyTopAndBottomRows, excludeInactiveProviders).ConfigureAwait(false));
        }

        [HttpGet]
        [Route("/ttmproductivity")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_FULL_ORGANIZATION_USERS)]
        public async Task<IActionResult> GetTtmProductivity(
            [BindRequired, FromQuery] Guid providerId,
            [BindRequired, FromQuery] int year,
            [BindRequired, FromQuery] int month,
            [Bind, FromQuery] bool byServiceDate = false
        )
        {
            if (!this._organizationContext.Provider.Any(x => x.Id == providerId))
                return this.NotFound();

            var builder = new TtmProductivityBuilder(this._organizationContext, this._reportContext);
            return this.Ok(await builder.BuildAsync(providerId, year, month, byServiceDate).ConfigureAwait(false));
        }

        [HttpGet]
        [Route("/productivitycompensation")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_FULL_ORGANIZATION_USERS)]
        public async Task<IActionResult> GetProductivityCompensation(
            [BindRequired, FromQuery] Guid providerId,
            [BindRequired, FromQuery] DateTime startDate,
            [BindRequired, FromQuery] DateTime endDate,
            [Bind, FromQuery] string accrualInterval = null,
            [Bind, FromQuery] bool byServiceDate = false
        )
        {
            if (!this._organizationContext.Provider.Any(x => x.Id == providerId))
                return this.NotFound();

            if (accrualInterval == null || !Enum.TryParse(accrualInterval, true, out AccrualInterval parsedInterval))
                parsedInterval = AccrualInterval.Annual;

            var builder = new ProductivityCompensationBuilder(this._organizationContext, this._reportContext, this._compensationContext);
            return this.Ok(await builder.BuildAsync(providerId, startDate, endDate, parsedInterval, byServiceDate).ConfigureAwait(false));
        }

        [HttpGet]
        [Route("/productivitycompensationcumulative")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_FULL_ORGANIZATION_USERS)]
        public async Task<IActionResult> GetProductivityCompensationCumulative(
            [BindRequired, FromQuery] Guid providerId,
            [BindRequired, FromQuery] DateTime startDate,
            [BindRequired, FromQuery] DateTime endDate,
            [Bind, FromQuery] string accrualInterval = null,
            [Bind, FromQuery] bool byServiceDate = false
        )
        {
            if (!this._organizationContext.Provider.Any(x => x.Id == providerId))
                return this.NotFound();

            if (accrualInterval == null || !Enum.TryParse(accrualInterval, true, out AccrualInterval parsedInterval))
                parsedInterval = AccrualInterval.Annual;

            var builder = new ProductivityCompensationBuilderCumulative(this._organizationContext, this._reportContext, this._compensationContext);
            return this.Ok(await builder.BuildAsync(providerId, startDate, endDate, parsedInterval, byServiceDate).ConfigureAwait(false));
        }

        [HttpGet]
        [Route("/compensationsummary")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_FULL_ORGANIZATION_USERS)]
        public async Task<IActionResult> GetCompensationSummary(
            [BindRequired, FromQuery] Guid providerId,
            [BindRequired, FromQuery] DateTime startDate,
            [BindRequired, FromQuery] DateTime endDate,
            [Bind, FromQuery] string accrualInterval = null,
            [Bind, FromQuery] bool byServiceDate = false
        )
        {
            if (!this._organizationContext.Provider.Any(x => x.Id == providerId))
                return this.NotFound();

            if (accrualInterval == null || !Enum.TryParse(accrualInterval, true, out AccrualInterval parsedInterval))
                parsedInterval = AccrualInterval.Annual;

            var builder = new CompensationSummaryBuilder(this._organizationContext, this._reportContext, this._compensationContext);
            return this.Ok(await builder.BuildAsync(providerId, startDate, endDate, parsedInterval, byServiceDate).ConfigureAwait(false));
        }

        [HttpGet]
        [Route("/compensationsummarycumulative")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_FULL_ORGANIZATION_USERS)]
        public async Task<IActionResult> GetCompensationSummaryCumulative(
            [BindRequired, FromQuery] Guid providerId,
            [BindRequired, FromQuery] DateTime startDate,
            [BindRequired, FromQuery] DateTime endDate,
            [Bind, FromQuery] string accrualInterval = null,
            [Bind, FromQuery] bool byServiceDate = false
        )
        {
            if (!this._organizationContext.Provider.Any(x => x.Id == providerId))
                return this.NotFound();

            if (accrualInterval == null || !Enum.TryParse(accrualInterval, true, out AccrualInterval parsedInterval))
                parsedInterval = AccrualInterval.Annual;

            var builder = new CompensationSummaryBuilderCumulative(this._organizationContext, this._reportContext, this._compensationContext);
            return this.Ok(await builder.BuildAsync(providerId, startDate, endDate, parsedInterval, byServiceDate).ConfigureAwait(false));
        }

        [HttpPost]
        [Route("/stackingreport")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_ADMINS_AND_HEALTHSYSTEM_USERS_AND_REGION_ADMINS_AND_REGION_USERS_AND_ORG_ADMINS)]
        public async Task<IActionResult> GetStackingReport(
             Guid providerId,
            [BindRequired, FromQuery] DateTime startDate,
            [BindRequired, FromQuery] DateTime endDate,
            [Bind, FromQuery] string accrualInterval = null
        )
        {
            if (!this._organizationContext.Provider.Any(t => t.Id == providerId))
                return this.NotFound();

            if (accrualInterval == null || !Enum.TryParse(accrualInterval, true, out AccrualInterval parsedInterval))
                parsedInterval = AccrualInterval.Annual;

            var builder = new StackingReportBuilder(this._organizationContext, this._reportContext, this._compensationContext, this._timeAndEffortDbContext);
            return this.Ok(await builder.BuildAsync(providerId, startDate, endDate, parsedInterval).ConfigureAwait(false));
        }

        [HttpPost]
        [Route("/accrualsummaryreport")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_ADMINS_AND_HEALTHSYSTEM_USERS_AND_REGION_ADMINS_AND_REGION_USERS_AND_ORG_ADMINS)]
        public async Task<IActionResult> GetAccrualSummaryReport(
            [BindRequired, FromQuery] Guid providerId,
            [BindRequired, FromQuery] DateTime startDate,
            [BindRequired, FromQuery] DateTime endDate,
            [Bind, FromQuery] string accrualInterval = null,
            [Bind, FromQuery] bool byServiceDate = false
        )
        {
            if (!this._organizationContext.Provider.Any(x => x.Id == providerId))
                return this.NotFound();

            if (accrualInterval == null || !Enum.TryParse(accrualInterval, true, out AccrualInterval parsedInterval))
                parsedInterval = AccrualInterval.Annual;

            var builder = new AccrualSummaryReportBuilder(this._organizationContext, this._reportContext, this._compensationContext);
            return this.Ok(await builder.BuildAsync(providerId, startDate, endDate, parsedInterval, byServiceDate).ConfigureAwait(false));
        }

        [HttpGet]
        [Route("/comparativesummary")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_ADMINS_AND_HEALTHSYSTEM_USERS_AND_REGION_ADMINS_AND_REGION_USERS_AND_ORG_ADMINS)]
        public async Task<IActionResult> GetComparativeSummary(
            [BindRequired, FromQuery] Guid selectedOrganizationId,
            [BindRequired, FromQuery] DateTime startDate,
            [BindRequired, FromQuery] DateTime endDate,
            [FromQuery] string specialty,
            [BindRequired, FromQuery] string metricType,
            [Bind, FromQuery] bool byServiceDate = true,
            [FromQuery] bool excludeInactiveProviders = true
        )
        {
            if (!await this.ValidateUserForSelectedOrganizationId(selectedOrganizationId) || endDate < startDate)
                return this.NotFound();


            var builder = new ComparativeSummaryBuilder(this._reportContext);
            string organizationName = await this._organizationContext.Organization.Where(org => org.Id == selectedOrganizationId).Select(org => org.Name).AsNoTracking().FirstOrDefaultAsync();
            return this.Ok(await builder.BuildAsync(selectedOrganizationId, specialty, startDate, endDate, metricType, organizationName, byServiceDate, excludeInactiveProviders).ConfigureAwait(false));
        }

        [HttpGet]
        [Route("/compensationhistoryreport")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_ADMINS_AND_HEALTHSYSTEM_USERS_AND_REGION_ADMINS_AND_REGION_USERS_AND_ORG_ADMINS)]
        public async Task<IActionResult> GetCompensationHistoryReport(
            [BindRequired, FromQuery] Guid providerId,
            [BindRequired, FromQuery] DateTime startDate,
            [BindRequired, FromQuery] DateTime endDate
        )
        {
            if (!this._organizationContext.Provider.Any(x => x.Id == providerId))
                return this.NotFound();

            var builder = new CompensationHistoryReportBuilder(this._organizationContext, this._reportContext, this._compensationContext);
            return this.Ok(await builder.BuildAsync(providerId, startDate, endDate).ConfigureAwait(false));
        }
        #endregion

        #region HealthSystem Reports
        [HttpGet]
        [Route("/get-providers-stats")]
        [Authorize(Policy = AuthorizationPolicies.ALLOW_ADMIN_HEALTHSYSTEM_USERS_AND_REGION_ADMINS_AND_REGION_USERS)]
        public async Task<IActionResult> GetProivdersStats(
           [BindRequired, FromQuery] short healthSystemId,
           [BindRequired, FromQuery] bool allRegions,
           [BindRequired, FromQuery] bool allOrganizations,
           [BindRequired, FromQuery] DateTime startDate,
           [BindRequired, FromQuery] DateTime endDate,
           [FromQuery] int? regionId,
           [FromQuery] Guid? organizationId
       )
        {
            var reportDto = new HealthSystemReportDto
            {
                HealthSystemId = healthSystemId,
                AllRegions = allRegions,
                AllOrganizations = allOrganizations,
                StartDate = startDate,
                EndDate = endDate,
                RegionId = regionId,
                OrganizationId = organizationId
            };
            if (!IsValidRequest(reportDto))
                return this.BadRequest();

            HealthSystemReportBuilder reportBuilder = new HealthSystemReportBuilder(this._compensationContext, this._organizationContext, this._evaluationContext);
            var regionsAndOrgs = this.GetRegionsAndOrganizations(reportDto);

            return this.Ok(await reportBuilder.GetProvidersStats(regionsAndOrgs, reportDto).ConfigureAwait(false));
        }

        #endregion

        #region Private Methods
        private async Task<bool> ValidateUserForSelectedOrganizationId(Guid selectedOrganizationId)
        {
            if (selectedOrganizationId != Guid.Empty)
            {
                var userId = this.HttpContext.User.FindFirstValue(ClaimTypes.NameIdentifier);
                if (string.IsNullOrEmpty(userId))
                    return false;

                var roleClaims = this._healthSystemAndRegionHelper.GetUserRoleClaims(this.HttpContext).Result;
                //To Handle Request for Admin, HealthSystem User, Region Admin and Users
                if (this._userRoleValidationHelper.HasValidAdminHSOrRegionRole(roleClaims, selectedOrganizationId))
                    return true;

                DateTime now = DateTime.UtcNow;
                Guid identityProviderId = new Guid(userId);
                var userRole = await this._authorizationContext.OrganizationRole.Where(or => or.IdentityProviderId == identityProviderId && or.OrganizationId == selectedOrganizationId && (or.EffectiveTo == null || or.EffectiveTo > now) && now > or.EffectiveFrom).Select(x => x.Role).FirstOrDefaultAsync().ConfigureAwait(false);

                if (userRole != 0 && userRole == Role.OrgAdmin)
                    return true;

            }
            return false;
        }

        private bool IsValidRequest(HealthSystemReportDto reportDto)
        {
            return reportDto != null &&
                reportDto.StartDate <= reportDto.EndDate &&
                (reportDto.AllRegions ||
                (!reportDto.AllRegions && reportDto.RegionId != null)) &&
                (reportDto.AllOrganizations ||
                (!reportDto.AllOrganizations && reportDto.OrganizationId != null));
        }

        private HealthSystemHierarchyResponse GetRegionsAndOrganizations(HealthSystemReportDto reportDto)
        {
            var roleClaims = GetRoleClaims();
            HealthSystemHierarchyResponse response = new HealthSystemHierarchyResponse();
            var currentUserRole = new RoleClaim();
            if (this._userRoleValidationHelper.HasAdminRole(roleClaims))
            {
                currentUserRole = roleClaims.FirstOrDefault();
                currentUserRole.HealthSystemHierarchies = currentUserRole.HealthSystemHierarchies.Where(hsh => hsh.HealthSystem.HealthSystemId == reportDto.HealthSystemId).ToList();
            }
            else
            {
                currentUserRole = this._userRoleValidationHelper.IsHealthSystemUser(roleClaims) ?
                    roleClaims.Where(rc => rc.HealthSystemId == reportDto.HealthSystemId).FirstOrDefault() :
                    roleClaims.Where(rc => rc.RegionId == reportDto.RegionId).FirstOrDefault();
            }



            if (currentUserRole == null || !currentUserRole.HealthSystemHierarchies.Any())
                return response;

            var currentHierarchy = currentUserRole.HealthSystemHierarchies.First();
            List<int> regionIds = new List<int>();
            if (!reportDto.AllRegions)
            {
                response.Regions.Add(currentHierarchy.Regions.Where(r => r.RegionId == (int)reportDto.RegionId).FirstOrDefault());
            }
            else
            {
                response.Regions.AddRange(currentHierarchy.Regions.ToList());
            }

            if (!reportDto.AllOrganizations)
            {
                response.Organizations.Add(currentHierarchy.Organizations.Where(o => o.Id == (Guid)reportDto.OrganizationId && o.RegionId == (!reportDto.AllRegions ? reportDto.RegionId : o.RegionId)).FirstOrDefault() ?? new OrganizationDto());
            }
            else
            {
                response.Organizations.AddRange(currentHierarchy.Organizations.Where(o => response.Regions.Select(r => r.RegionId).ToList().Contains((int)o.RegionId)).ToList());
            }

            return response;
        }

        private List<RoleClaim> GetRoleClaims()
        {
            return this._healthSystemAndRegionHelper.GetUserRoleClaims(this.HttpContext).Result;
        }
        #endregion
    }
}
