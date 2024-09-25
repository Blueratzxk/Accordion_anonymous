//
// Created by anonymous on 7/19/24.
//

#ifndef OLVP_STRINGUTILS_HPP
#define OLVP_STRINGUTILS_HPP

#include <string>
#include <vector>
#include <sstream>


class StringUtils
{
    static void Stringsplit(std::string str, const const char split,std::vector<std::string>& res)
    {
        std::istringstream iss(str);
        std::string token;
        while (getline(iss, token, split))
        {
            res.push_back(token);
        }
    }



};

#endif //OLVP_STRINGUTILS_HPP
