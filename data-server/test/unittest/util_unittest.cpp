
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <algorithm>

#include "base/util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore;

TEST(Util, Hex) {
    std::string str;
    for (int i = 0; i <= 0xFF; ++i) {
        str.push_back(static_cast<char>(i));
    }
    std::string expected_hex;
    for (int i = 0; i <= 0xFF; ++i) {
        char buf[3];
        snprintf(buf, 3, "%02X", static_cast<unsigned char>(i));
        expected_hex.append(buf, 2);
    }
    std::string hex = EncodeToHex(str);
    ASSERT_EQ(hex, expected_hex);
    std::cout << "hex: " << hex << std::endl;

    std::string decoded_str;
    auto ret = DecodeFromHex(hex, &decoded_str);
    ASSERT_TRUE(ret);
    ASSERT_EQ(decoded_str, str);

    ret = DecodeFromHex(hex + "a", &decoded_str);
    ASSERT_FALSE(ret);

    ret = DecodeFromHex(hex, nullptr);
    ASSERT_FALSE(ret);

    for (int i = 0; i < 100; ++i) {
        std::string str = randomString(randomInt() % 100 + 100);
        std::string hex = EncodeToHex(str);
        std::string str2;
        auto ret = DecodeFromHex(hex, &str2);
        ASSERT_EQ(str2, str);
    }
}


TEST(Util, SliceSeparate) {
    int times = 50;
    int err = 0;
    while (times--) {
        std::string pre = randomString(randomInt() % 10 + 10);
        std::vector<std::string> keys;

        int len = 4 + randomInt() % 100;
        for (int i=0; i<len; i++) {
            keys.push_back(pre+randomString(randomInt() % 500));
        }

        std::sort(keys.begin(), keys.end());

        std::string sk = keys[0];
        std::string ek = keys[len-1];

        std::string lk = keys[len/2-1];
        std::string rk = keys[len/2];
        std::string sp = SliceSeparate(lk, rk, sk.length() + 5);

        for (int i=0; i<len; i++) {
            if (sp < keys[i]) {
                int k = len / 2 - i;
                if (k > 10 || k < -10) {
                    ++err;
                }
                break;
            }
        }
        ASSERT_GE(sp, sk) << "\nsk:" << sk << "\nlk:" << lk << "\nrk:" << rk << "\nek:" << ek << std::endl;
        ASSERT_LE(sp, ek) << "\nsk:" << sk << "\nlk:" << lk << "\nrk:" << rk << "\nek:" << ek << std::endl;
    }

    std::cout << "offset > 10: " << err  << " ratio:"<< err * 1.0 / 1000 <<"%" << std::endl;
}

TEST(Util, NextComparable) {
    {
        std::string str;
        auto ret = NextComparable(str);
        ASSERT_EQ(ret, "");
    }
    {
        std::string str = "a";
        auto ret = NextComparable(str);
        ASSERT_EQ(ret, "b");
    }
    {
        std::string str = "abcdef";
        auto ret = NextComparable(str);
        ASSERT_EQ(ret, "abcdeg");
    }
    {
        std::string str = "\xFF";
        auto ret = NextComparable(str);
        ASSERT_EQ(ret, "");
    }
    {
        std::string str = "abc\xFF\xFF";
        auto ret = NextComparable(str);
        ASSERT_EQ(ret, "abd");
    }
    {
        std::string str = "\x01\x02\xFF\xFF\xFF";
        auto ret = NextComparable(str);
        ASSERT_EQ(ret, "\x01\x03");
    }
    {
        std::string str = "\x01\x02\xFF\xFE\xFF";
        auto ret = NextComparable(str);
        ASSERT_EQ(ret, "\x01\x02\xFF\xFF");
    }
}

} /* namespace  */
