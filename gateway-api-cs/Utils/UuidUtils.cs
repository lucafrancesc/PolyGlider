using System;

namespace GatewayApiCs.Utils;

public static class UuidUtils
{
    public static bool IsValidUuid(string s)
    {
        return Guid.TryParse(s, out _);
    }
}
