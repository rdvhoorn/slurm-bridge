// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmjobir

import (
	"errors"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var (
	ErrorInsuffientPods     = errors.New("not enough pending pods to create external job")
	ErrorExternalJobInvalid = errors.New("not enough pending pods for created external job")
)

func ConvStrTo32(input string) (output *int32, err error) {
	out, err := strconv.ParseInt(input, 10, 32)
	if err != nil {
		return nil, err
	}
	retVal := int32(out)

	return &retVal, err
}

func ParseSlurmJobId(input string) int32 {
	out, err := strconv.ParseUint(input, 10, 32)
	if err != nil {
		return 0
	}
	return int32(out) //nolint:gosec // disable G115
}

// GetMemoryFromQuantity converts quantity to MiB for the slurm job IR,
// rounding up so the Slurm reservation is never below the Kubernetes value.
func GetMemoryFromQuantity(quantity *resource.Quantity) int64 {
	const mebibyte = 1024 * 1024
	val := quantity.Value()
	mib := val / mebibyte
	if val%mebibyte > 0 {
		mib++
	}
	return mib
}

func getFirstPod(p corev1.PodList) *corev1.Pod {
	if len(p.Items) > 0 {
		return &p.Items[0]
	}
	return nil
}
