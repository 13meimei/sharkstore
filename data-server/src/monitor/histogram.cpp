// Copyright (c) 2018 The SharkStore Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "histogram.h"

#include <limits>
#include <vector>
#include <map>

namespace sharkstore {
namespace monitor {

class HistogramBucketMapper {
public:
    HistogramBucketMapper();

    // converts a value to the bucket index.
    size_t IndexForValue(uint64_t value) const;
    // number of buckets required.

    size_t BucketCount() const {
        return bucketValues_.size();
    }

    uint64_t LastValue() const {
        return maxBucketValue_;
    }

    uint64_t FirstValue() const {
        return minBucketValue_;
    }

    uint64_t BucketLimit(const size_t bucketNumber) const {
        assert(bucketNumber < BucketCount());
        return bucketValues_[bucketNumber];
    }

private:
    std::vector<uint64_t> bucketValues_;
    uint64_t maxBucketValue_;
    uint64_t minBucketValue_;
    std::map<uint64_t, uint64_t> valueIndexMap_;
};

HistogramBucketMapper::HistogramBucketMapper() {
    // If you change this, you also need to change
    // size of array buckets_ in HistogramImpl
    bucketValues_ = {1, 2};
    valueIndexMap_ = {{1, 0}, {2, 1}};
    double bucket_val = static_cast<double>(bucketValues_.back());
    while ((bucket_val = 1.5 * bucket_val) <= static_cast<double>(std::numeric_limits<uint64_t>::max())) {
        bucketValues_.push_back(static_cast<uint64_t>(bucket_val));
        // Extracts two most significant digits to make histogram buckets more
        // human-readable. E.g., 172 becomes 170.
        uint64_t pow_of_ten = 1;
        while (bucketValues_.back() / 10 > 10) {
            bucketValues_.back() /= 10;
            pow_of_ten *= 10;
        }
        bucketValues_.back() *= pow_of_ten;
        valueIndexMap_[bucketValues_.back()] = bucketValues_.size() - 1;
    }
    maxBucketValue_ = bucketValues_.back();
    minBucketValue_ = bucketValues_.front();
}

size_t HistogramBucketMapper::IndexForValue(const uint64_t value) const {
    if (value >= maxBucketValue_) {
        return bucketValues_.size() - 1;
    } else if ( value >= minBucketValue_ ) {
        std::map<uint64_t, uint64_t>::const_iterator lowerBound =
                valueIndexMap_.lower_bound(value);
        if (lowerBound != valueIndexMap_.end()) {
            return static_cast<size_t>(lowerBound->second);
        } else {
            return 0;
        }
    } else {
        return 0;
    }
}

static const HistogramBucketMapper bucketMapper;




}  // namespace monitor
}  // namespace sharkstore
