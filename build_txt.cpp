// [[Rcpp::depends(BH)]]

#include<Rcpp.h>
#include <boost/lexical_cast.hpp>

using namespace Rcpp;

// [[Rcpp::export]]

Rcpp::CharacterVector build_txt(NumericMatrix XY, NumericMatrix VALS, long ID) {
    CharacterVector out(XY.nrow());

    for(int i=0; i < XY.nrow(); i++) {
        String this_out;
        this_out += boost::lexical_cast<std::string>(ID) + "|";
        // Build point coords WKT string
        this_out += "POINT(" + boost::lexical_cast<std::string>(XY(i, 0)) + " " + boost::lexical_cast<std::string>(XY(i, 1)) + ")";
        for (int j=0; j < VALS.ncol(); j++) {
            this_out += "|";
            if (std::isnan(VALS(i, j))) {
                this_out += "-32768";
            } else {
                this_out += boost::lexical_cast<std::string>(VALS(i, j));
            }
        }
        out(i) = this_out;
        ID++;
    }
    return out;
}
