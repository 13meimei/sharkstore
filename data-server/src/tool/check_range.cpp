
#include <string>
#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <vector>
#include <assert.h>

int main(int argc, char **argv)
{
    std::fstream fs;

    fs.open(argv[1]);

    if(!fs.is_open()) {
        std::cout << "not open \n";
        return 0;
    }

    std::map <std::string, std::pair<std::string,std::string>> ranges;
    std::string bs = "key range[";
    std::string rs = "range[";

    for (std::string line; std::getline(fs, line); ) {
        auto b = line.find(rs) + rs.length();
        auto e = line.find(' ', b);
        auto r = line.substr(b, e - b);

        b = line.find(bs) + bs.length();
        e = line.find(' ', b);
        auto k = line.substr(b, e - b);

        b = line.rfind(' ') + 1;
        e = line.rfind(']');
        auto v = line.substr(b, e - b);

        assert(k < v);
        ranges[k] = std::make_pair(r, v);
    }
    std::vector<std::tuple<std::string, std::string, std::string>> vr;

    auto it = ranges.begin();
    while (it != ranges.end()) {
        vr.push_back (std::make_tuple(it->second.first, it->first, it->second.second));
        it = ranges.find(it->second.second);
    }

    for (auto &vit : vr) {
        std::cout << "Range["<<std::get<0>(vit) << "] : [" << std::get<1>(vit) 
            << " - " << std::get<2>(vit) << "]" << std::endl;
        ranges.erase(std::get<1>(vit));
    }

    if (!ranges.empty()) {
        std::cout << "\n\nDiscontinuous " << std::endl;
    }

    for (auto &it : ranges) {
        std::cout << "Range["<< it.second.first << "] : [" << it.first
            << " - " << it.second.second << "]" << std::endl;
    }

    fs.close();
}
