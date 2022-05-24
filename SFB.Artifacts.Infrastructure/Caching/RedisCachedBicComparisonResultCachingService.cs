using Newtonsoft.Json;
using SFB.Web.ApplicationCore.Models;
using SFB.Web.ApplicationCore.Services;
using StackExchange.Redis;
using System;
using System.Configuration;
using System.Net;
using System.Net.Http;
using System.Runtime.Caching;
using System.Threading.Tasks;

namespace SFB.Web.Infrastructure.Caching
{
    public class RedisCachedBicComparisonResultCachingService : IBicComparisonResultCachingService
    {

        private static HttpClient _client;

        public RedisCachedBicComparisonResultCachingService(HttpClient client)
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
        
        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            string cacheConnection = ConfigurationManager.AppSettings["RedisConnectionString"].ToString();
            return ConnectionMultiplexer.Connect(cacheConnection);
        });

        public static ConnectionMultiplexer Connection => lazyConnection.Value;
        
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

        public ComparisonResult GetBicComparisonResultByUrn(long urn)
        {
            var cache = Connection.GetDatabase();

            var serializedList = cache.StringGet("BicComparisonResult-"+urn);

            ComparisonResult deserializedList = null;

            if (!serializedList.IsNull)
            {
                deserializedList = JsonConvert.DeserializeObject<ComparisonResult>(serializedList);
            }

            return deserializedList;
        }

        public void StoreBicComparisonResultByUrn(long urn, ComparisonResult comparisonResult)
        {
            var cache = Connection.GetDatabase();

            cache.StringSet("BicComparisonResult-" + urn, JsonConvert.SerializeObject(comparisonResult));
        }
    }
}
