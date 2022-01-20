using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Caching;
using Newtonsoft.Json;
using SFB.Web.ApplicationCore.Services;

namespace SFB.Web.Infrastructure.Caching
{
    public class NetCoreCachedLocalAuthoritiesService : ILocalAuthoritiesService
    {
        private readonly MemoryCache _memoryCache;
        private dynamic _list;

        public NetCoreCachedLocalAuthoritiesService(dynamic list)
        {
            _list = list;
            _memoryCache = MemoryCache.Default;
            CacheItemPolicy policy = new CacheItemPolicy();
            policy.SlidingExpiration = TimeSpan.FromDays(1);
            _memoryCache.Set("SFBLARegionList", list, policy);
        }

        public dynamic GetLocalAuthorities()
        {
            var list = (dynamic)_memoryCache.Get("SFBLARegionList");

            if (list is null)
            {
                CacheItemPolicy policy = new CacheItemPolicy();
                policy.SlidingExpiration = TimeSpan.FromDays(1);
                _memoryCache.Set("SFBLARegionList", _list, policy);
            }

            return list;
        }

        public string GetLaName(string laCode)
        {
            var localAuthorities = (List<dynamic>)JsonConvert.DeserializeObject<List<dynamic>>(GetLocalAuthorities());
            var localAuthourity = localAuthorities.FirstOrDefault(la => la.id == laCode);
            return localAuthourity == null ? string.Empty : localAuthourity.LANAME;
        }
    }
}