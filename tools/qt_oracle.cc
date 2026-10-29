// Oracle: reads commands on stdin, prints results from Overpass's own index math.
//   N lat lon            -> node index
//   W k idx1 .. idxk     -> calc_index
//   R s n w e            -> calc_ranges (lo hi pairs)
//   P k lo1 hi1 ..       -> calc_parents of node ranges
#include "overpass_api/core/index_computations.h"
#include "template_db/ranges.def.h"
#include <iostream>
#include <string>
int main() {
  std::string cmd;
  while (std::cin >> cmd) {
    if (cmd == "N") { double la, lo; std::cin >> la >> lo; std::cout << ll_upper_(la, lo) << "\n"; }
    else if (cmd == "W") { int k; std::cin >> k; std::vector<uint32> v(k); for (auto& x : v) std::cin >> x;
      std::cout << calc_index(v) << "\n"; }
    else if (cmd == "R") { double s, n, w, e; std::cin >> s >> n >> w >> e;
      Ranges<Uint32_Index> r = calc_ranges(s, n, w, e);
      for (auto it = r.begin(); it != r.end(); ++it) std::cout << it.lower_bound().val() << " " << it.upper_bound().val() << " ";
      std::cout << "\n"; }
    else if (cmd == "P") { int k; std::cin >> k; Ranges<Uint32_Index> in;
      for (int i = 0; i < k; ++i) { uint32 a, b; std::cin >> a >> b; in.push_back(a, b); } in.sort();
      Ranges<Uint31_Index> r = calc_parents(in);
      for (auto it = r.begin(); it != r.end(); ++it) std::cout << it.lower_bound().val() << " " << it.upper_bound().val() << " ";
      std::cout << "\n"; }
  }
}
