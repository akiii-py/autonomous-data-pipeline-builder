package handlers

import "testing"

func TestParseLimit(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		fallback int
		max      int
		want     int
		wantErr  bool
	}{
		{name: "default", raw: "", fallback: 50, max: 200, want: 50},
		{name: "valid", raw: "20", fallback: 50, max: 200, want: 20},
		{name: "clamped", raw: "999", fallback: 50, max: 200, want: 200},
		{name: "invalid text", raw: "abc", fallback: 50, max: 200, wantErr: true},
		{name: "invalid non positive", raw: "0", fallback: 50, max: 200, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLimit(tc.raw, tc.fallback, tc.max)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}
}

func TestParseOffset(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{name: "default", raw: "", want: 0},
		{name: "valid", raw: "12", want: 12},
		{name: "invalid text", raw: "abc", wantErr: true},
		{name: "invalid negative", raw: "-1", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseOffset(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}
}

func TestIsValidRunStatus(t *testing.T) {
	valid := []string{"pending", "running", "completed", "failed", "skipped"}
	for _, s := range valid {
		if !isValidRunStatus(s) {
			t.Fatalf("expected status %q to be valid", s)
		}
	}

	invalid := []string{"", "draft", "done"}
	for _, s := range invalid {
		if isValidRunStatus(s) {
			t.Fatalf("expected status %q to be invalid", s)
		}
	}
}

func TestIsValidEventLevel(t *testing.T) {
	valid := []string{"info", "warn", "error"}
	for _, s := range valid {
		if !isValidEventLevel(s) {
			t.Fatalf("expected level %q to be valid", s)
		}
	}

	invalid := []string{"", "warning", "fatal", "INFO"}
	for _, s := range invalid {
		if isValidEventLevel(s) {
			t.Fatalf("expected level %q to be invalid", s)
		}
	}
}
