#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <bitset>
#include <random>

namespace qp {

namespace {

// A utility class which provides uniformly distributed random numbers seeded
// with the hash function on a given input.  Useful for generating multiple
// bloomfilter bit indexes for a key.
template <typename T, int Size, typename Hash = std::hash<T>>
struct Mixer {
  std::minstd_rand rng_;
  Mixer(const T& val) : rng_(Hash()(val)) {}
  std::size_t operator()() { return rng_() % Size; }
};

}  // namespace

// A probabilistic space efficient data structure used for testing membership in
// a set.
// https://en.wikipedia.org/wiki/Bloom_filter
template <typename Key, int Size, int NumHashes, typename Hash = std::hash<Key>>
class Bloomfilter {
 public:
  Bloomfilter() = default;

  Bloomfilter(const std::initializer_list<Key>& init) {
    for (const auto& key : init) {
      Add(key);
    }
  }

  constexpr int size() const { return Size; }

  void Add(const Key& key) {
    Mixer<Key, Size, Hash> mixer(key);
    for (int i = 0; i < NumHashes; ++i) {
      bits_.set(mixer());
    }
  }

  bool MaybeContains(const Key& key) const {
    Mixer<Key, Size, Hash> mixer(key);
    for (int i = 0; i < NumHashes; ++i) {
      if (!bits_[mixer()]) return false;
    }
    return true;
  }

 private:
  std::bitset<Size> bits_;
};

}  // namespace qp

#endif /* BLOOMFILTER_H */
