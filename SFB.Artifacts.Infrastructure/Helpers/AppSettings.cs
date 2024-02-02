namespace SFB.Artifacts.Infrastructure.Helpers
{
    public static class AppSettings
    {
        public struct CosmosConnectionMode
        {
            public const string Key = nameof(CosmosConnectionMode);
            public const string Direct = nameof(Direct);
            public const string Gateway = nameof(Gateway);
        }
    }
}