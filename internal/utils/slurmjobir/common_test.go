// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmjobir

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func Test_ConvStrTo32(t *testing.T) {
	type args struct {
		input string
	}
	var num int32 = 1234
	tests := []struct {
		name       string
		args       args
		wantOutput *int32
		wantErr    bool
	}{
		{
			name: "Is A Number",
			args: args{
				input: "1234",
			},
			wantOutput: &num,
			wantErr:    false,
		},
		{
			name: "Not A Number",
			args: args{
				"1234.eu",
			},
			wantOutput: nil,
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOutput, err := ConvStrTo32(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvStrTo32() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if (err == nil) && (*gotOutput != *tt.wantOutput) {
				t.Errorf("ConvStrTo32() = %d, want %d", *gotOutput, *tt.wantOutput)
			}
		})
	}
}

func Test_ParseSlurmJobId(t *testing.T) {
	type args struct {
		input string
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		{
			name: "Number",
			args: args{
				input: "1234",
			},
			want: 1234,
		},
		{
			name: "Negative Number",
			args: args{
				input: "-1234",
			},
			want: 0,
		},
		{
			name: "Invalid",
			args: args{
				input: "asdf",
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseSlurmJobId(tt.args.input); got != tt.want {
				t.Errorf("ParseSlurmJobId() = %d, want %d", got, tt.want)
			}
		})
	}
}

func Test_GetMemoryFromQuantity(t *testing.T) {
	type args struct {
		input resource.Quantity
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "Number",
			args: args{
				input: *resource.NewQuantity(1024*1024, resource.DecimalSI),
			},
			want: 1,
		},
		{
			name: "Exact MiB",
			args: args{
				input: resource.MustParse("100Mi"),
			},
			want: 100,
		},
		{
			name: "Fractional MiB rounds up",
			args: args{
				input: resource.MustParse("100.5Mi"),
			},
			want: 101,
		},
		{
			name: "One byte over MiB rounds up",
			args: args{
				input: *resource.NewQuantity(1024*1024+1, resource.BinarySI),
			},
			want: 2,
		},
		{
			name: "Below one MiB rounds up",
			args: args{
				input: resource.MustParse("1k"),
			},
			want: 1,
		},
		{
			name: "Zero",
			args: args{
				input: resource.MustParse("0"),
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetMemoryFromQuantity(&tt.args.input); got != tt.want {
				t.Errorf("ParseSlurmJobId() = %d, want %d", got, tt.want)
			}
		})
	}
}
