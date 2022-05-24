using System.Threading.Tasks;

namespace SFB.Web.Infrastructure.Caching
{
    public interface IExternalServiceLookup
    {
        Task<bool> CscpHasPage(int urn, bool isMat);
        Task<bool> GiasHasPage(int urn, bool isMat);
    }
}