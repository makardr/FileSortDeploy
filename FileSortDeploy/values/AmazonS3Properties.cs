namespace FileSortDeploy.values;

public class AmazonS3Properties(string bucketName, string prefix, string accessKey, string secretKey, string serviceUrl)
{
    public string BucketName { get; set; } = bucketName;
    public string Prefix { get; set; } = prefix;
    public string AccessKey { get; set; } = accessKey;
    public string SecretKey { get; set; } = secretKey;
    public string ServiceUrl { get; set; } = serviceUrl;
}