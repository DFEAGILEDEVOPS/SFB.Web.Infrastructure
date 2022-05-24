using System;
using System.Configuration;
using System.Net;
using System.Net.Http;
using System.Runtime.Caching;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace SFB.Web.Infrastructure.Caching
{
    public class ExternalServiceLookup : IExternalServiceLookup
    {
        private static HttpClient _client;
        
        public ExternalServiceLookup(HttpClient client)
        {
            _client = client;
        }
        
        private string _matKeyFragment = "multi-academy-trust";
        private string _schoolKeyFragment = "school";
        private string GetCollection(bool mat)
        {
            var collection = mat ? _matKeyFragment : _schoolKeyFragment;
            return collection;
        }

        private static readonly Lazy<ConnectionMultiplexer> LazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            string cacheConnection = ConfigurationManager.AppSettings["RedisConnectionString"];
            return ConnectionMultiplexer.Connect(cacheConnection);
        });

        public static ConnectionMultiplexer Connection => LazyConnection.Value;

        public async Task<bool> CscpHasPage(int urn, bool isMat)
        {
            var collection = GetCollection(isMat);
            var key = $"cscp-{collection}-{urn}";
            var value = MemoryCache.Default.Get(key);

            if (value != null)
            {
                return (bool) value;
            }
            else
            {
                var baseUrl = ConfigurationManager.AppSettings["SptApiUrl"];
                
                baseUrl = isMat ?
                    $"{baseUrl}/multi-academy-trust/{urn}": 
                    $"{baseUrl}/school/{urn}";
                
                var request = new HttpRequestMessage(HttpMethod.Head, $"{baseUrl}");

                var result = await _client.SendAsync(request);

                var isOk = result.StatusCode == HttpStatusCode.OK;
                
                MemoryCache.Default.Set(
                    new CacheItem(key, isOk), 
                    new CacheItemPolicy{AbsoluteExpiration = DateTimeOffset.Now.AddHours(24)}
                );

                return isOk;
            }
        }

        public async Task<bool> GiasHasPage(int urn, bool isMat)
        {
            var collection = GetCollection(isMat);
            var key = $"gias-{collection}-{urn}";
            var value = MemoryCache.Default.Get(key);
            
            if (value != null)
            {
                return (bool) value;
            }
            else
            {
                var baseUrl = ConfigurationManager.AppSettings["GiasApiUrl"];
                
                baseUrl = isMat ?
                    $"{baseUrl}/Groups/Group/Detail/{urn}": 
                    $"{baseUrl}/Establishments/Establishment/Details/{urn}";
                
                var request = new HttpRequestMessage(HttpMethod.Head, $"{baseUrl}");

                var result = await _client.SendAsync(request);

                var isOk = result.StatusCode == HttpStatusCode.OK;
                
                MemoryCache.Default.Set(
                    new CacheItem(key, isOk), 
                    new CacheItemPolicy{AbsoluteExpiration = DateTimeOffset.Now.AddHours(24)}
                );

                return isOk;
            }
        }
    }
}