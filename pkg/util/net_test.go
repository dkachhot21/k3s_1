package util

import (
	"net"
	"reflect"
	"testing"
)

func Test_UnitParseStringSliceToIPs(t *testing.T) {
	tests := []struct {
		name    string
		arg     []string
		want    []net.IP
		wantErr bool
	}{
		{
			name: "nil string slice must return no errors",
			arg:  nil,
			want: nil,
		},
		{
			name: "empty string slice must return no errors",
			arg:  []string{},
			want: nil,
		},
		{
			name: "single element slice with correct IP must succeed",
			arg:  []string{"10.10.10.10"},
			want: []net.IP{net.ParseIP("10.10.10.10")},
		},
		{
			name: "single element slice with correct IP list must succeed",
			arg:  []string{"10.10.10.10,10.10.10.11"},
			want: []net.IP{
				net.ParseIP("10.10.10.10"),
				net.ParseIP("10.10.10.11"),
			},
		},
		{
			name: "multi element slice with correct IP list must succeed",
			arg:  []string{"10.10.10.10,10.10.10.11", "10.10.10.12,10.10.10.13"},
			want: []net.IP{
				net.ParseIP("10.10.10.10"),
				net.ParseIP("10.10.10.11"),
				net.ParseIP("10.10.10.12"),
				net.ParseIP("10.10.10.13"),
			},
		},
		{
			name:    "single element slice with correct IP list with trailing comma must fail",
			arg:     []string{"10.10.10.10,"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "single element slice with incorrect IP (overflow) must fail",
			arg:     []string{"10.10.10.256"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "single element slice with incorrect IP (foreign symbols) must fail",
			arg:     []string{"xxx.yyy.zzz.www"},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := ParseStringSliceToIPs(tt.arg)
				if (err != nil) != tt.wantErr {
					t.Errorf("ParseStringSliceToIPs() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ParseStringSliceToIPs() = %v, want %v", got, tt.want)
				}
			},
		)
	}
}

func Test_UnitGetDefaultDualStackAddresses(t *testing.T) {
	bind, cluster, service, err := GetDefaultDualStackAddresses([]net.IP{net.ParseIP("192.168.1.50")})
	if err != nil {
		t.Fatalf("GetDefaultDualStackAddresses() unexpected error: %v", err)
	}
	if bind != "0.0.0.0" {
		t.Errorf("GetDefaultDualStackAddresses() bind = %v, want 0.0.0.0", bind)
	}
	if cluster != "10.42.0.0/16,fd00:42::/56" {
		t.Errorf("GetDefaultDualStackAddresses() cluster = %v, want 10.42.0.0/16,fd00:42::/56", cluster)
	}
	if service != "10.43.0.0/16,fd00:43::/112" {
		t.Errorf("GetDefaultDualStackAddresses() service = %v, want 10.43.0.0/16,fd00:43::/112", service)
	}

	bindv6, clusterv6, servicev6, err := GetDefaultDualStackAddresses([]net.IP{net.ParseIP("2001:db8::1")})
	if err != nil {
		t.Fatalf("GetDefaultDualStackAddresses() unexpected error: %v", err)
	}
	if bindv6 != "::" {
		t.Errorf("GetDefaultDualStackAddresses() bindv6 = %v, want ::", bindv6)
	}
	if clusterv6 != "fd00:42::/56,10.42.0.0/16" {
		t.Errorf("GetDefaultDualStackAddresses() clusterv6 = %v, want fd00:42::/56,10.42.0.0/16", clusterv6)
	}
	if servicev6 != "fd00:43::/112,10.43.0.0/16" {
		t.Errorf("GetDefaultDualStackAddresses() servicev6 = %v, want fd00:43::/112,10.43.0.0/16", servicev6)
	}
}

func Test_UnitCheckDualStackSupported(t *testing.T) {
	err := CheckDualStackSupported([]net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")})
	t.Logf("CheckDualStackSupported() result: %v", err)
}

func Test_UnitValidateNoCIDRConflict(t *testing.T) {
	tests := []struct {
		name         string
		clusterCIDRs []string
		serviceCIDRs []string
		wantErr      bool
	}{
		{
			name:         "empty CIDRs must return no error",
			clusterCIDRs: nil,
			serviceCIDRs: nil,
			wantErr:      false,
		},
		{
			name:         "reserved RFC documentation CIDRs must succeed on normal hosts",
			clusterCIDRs: []string{"198.51.100.0/24", "2001:db8:ffff::/48"},
			serviceCIDRs: []string{"203.0.113.0/24", "2001:db8:eeee::/48"},
			wantErr:      false,
		},
		{
			name:         "invalid CIDR string should skip parsing without panic",
			clusterCIDRs: []string{"invalid-cidr"},
			serviceCIDRs: nil,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNoCIDRConflict(tt.clusterCIDRs, tt.serviceCIDRs)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateNoCIDRConflict() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// Dynamic test: test that an active non-loopback host IP triggers conflict detection
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && !ip.IsLoopback() {
				mask := "/32"
				if ip.To4() == nil {
					mask = "/128"
				}
				conflictCIDR := ip.String() + mask
				if err := ValidateNoCIDRConflict([]string{conflictCIDR}, nil); err == nil {
					t.Errorf("ValidateNoCIDRConflict() expected conflict for host IP %s, got nil", conflictCIDR)
				}
				break
			}
		}
	}
}


