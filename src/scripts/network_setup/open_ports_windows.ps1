# Check if running as administrator
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Warning "Please run as Administrator"
    exit 1
}

# Array of port configurations
$ports = @(
    @{Port = 5555; Name = "Ether PubSub Frontend"},
    @{Port = 5556; Name = "Ether PubSub Backend"},
    @{Port = 5559; Name = "Ether ReqRep Frontend"},
    @{Port = 5560; Name = "Ether ReqRep Backend"},
    @{Port = 6379; Name = "Ether Redis"}
)

# Add firewall rules for each port
foreach ($config in $ports) {
    Write-Host "Adding rule for $($config.Name) on port $($config.Port)..."
    
    New-NetFirewallRule -DisplayName $config.Name `
                       -Direction Inbound `
                       -LocalPort $config.Port `
                       -Protocol TCP `
                       -Action Allow
}

Write-Host "`nFirewall rules added. Current Ether rules:"
Get-NetFirewallRule | Where-Object { $_.DisplayName -like "Ether*" } | Format-Table DisplayName, Enabled, Direction, Action 