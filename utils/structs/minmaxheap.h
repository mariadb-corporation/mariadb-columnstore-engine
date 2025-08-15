#include <vector>
#include <stdexcept>
#include <utility>      // std::swap
#include <algorithm>    // std::min_element, std::max_element

template <typename T, typename Compare = std::less<T>>
class MinMaxHeap {
public:
    // --- интерфейс ---
    bool empty() const noexcept { return data_.empty(); }
    std::size_t size() const noexcept { return data_.size(); }

    const T& getMin() const {
        if (empty()) throw std::out_of_range("heap empty");
        return data_[0];
    }

    const T& getMax() const {
        if (empty()) throw std::out_of_range("heap empty");
        if (data_.size() == 1) return data_[0];
        if (data_.size() == 2) return data_[1];
        return cmp_(data_[1], data_[2]) ? data_[2] : data_[1]; // whichever is larger
    }

    void push(const T& x) {
        data_.push_back(x);
        bubbleUp(data_.size() - 1);
    }

    void popMin() {
        if (empty()) throw std::out_of_range("heap empty");
        moveLastToRootThenTrickleDown(0);
    }

    void popMax() {
        if (empty()) throw std::out_of_range("heap empty");
        std::size_t maxi = maxIndexUnderRoot();
        moveLastToRootThenTrickleDown(maxi);
    }

private:
    // --- внутренние данные ---
    std::vector<T> data_;
    Compare cmp_{};                // по умолчанию std::less ⇒ min на чётных уровнях

    // --- индексы ---
    static std::size_t parent(std::size_t i)     { return (i - 1) / 2; }
    static std::size_t grand(std::size_t i)      { return (i - 1) / 4; }
    static std::size_t left  (std::size_t i)     { return 2 * i + 1; }
    static std::size_t right (std::size_t i)     { return 2 * i + 2; }

    static bool isMinLevel(std::size_t i) { return (std::bit_width(i + 1) & 1) == 1; }
    // bit_width(n) ≡ floor(log2(n)) + 1  (C++20 <bit>)

    // bubble-up (вставка)
    void bubbleUp(std::size_t i) {
        if (i == 0) return;
        std::size_t p = parent(i);
        if (isMinLevel(i)) {
            if (!cmp_(data_[i], data_[p])) {               // x > parent ⇒ уровень макс <-->
                std::swap(data_[i], data_[p]);
                bubbleUpMax(p);
            } else {
                bubbleUpMin(i);
            }
        } else {                                           // max-уровень
            if (cmp_(data_[i], data_[p])) {                // x < parent ⇒ уровень мин <-->
                std::swap(data_[i], data_[p]);
                bubbleUpMin(p);
            } else {
                bubbleUpMax(i);
            }
        }
    }

    void bubbleUpMin(std::size_t i) {
        while (i >= 3) {
            std::size_t g = grand(i);
            if (cmp_(data_[i], data_[g])) {
                std::swap(data_[i], data_[g]);
                i = g;
            } else break;
        }
    }

    void bubbleUpMax(std::size_t i) {
        while (i >= 3) {
            std::size_t g = grand(i);
            if (!cmp_(data_[i], data_[g])) {
                std::swap(data_[i], data_[g]);
                i = g;
            } else break;
        }
    }

    // trickle-down (удаление)
    void trickleDown(std::size_t i) {
        if (isMinLevel(i))
            trickleDownMin(i);
        else
            trickleDownMax(i);
    }

    void trickleDownMin(std::size_t i) {
        while (left(i) < data_.size()) {
            std::size_t m = minDescendant(i);
            if (isGrandchild(i, m)) {
                if (cmp_(data_[m], data_[i])) std::swap(data_[m], data_[i]);
                std::size_t p = parent(m);
                if (!cmp_(data_[m], data_[p])) std::swap(data_[m], data_[p]);
                i = m;
            } else {    // ребёнок
                if (cmp_(data_[m], data_[i])) std::swap(data_[m], data_[i]);
                break;
            }
        }
    }

    void trickleDownMax(std::size_t i) {
        while (left(i) < data_.size()) {
            std::size_t m = maxDescendant(i);
            if (isGrandchild(i, m)) {
                if (!cmp_(data_[m], data_[i])) std::swap(data_[m], data_[i]);
                std::size_t p = parent(m);
                if (cmp_(data_[m], data_[p])) std::swap(data_[m], data_[p]);
                i = m;
            } else {
                if (!cmp_(data_[m], data_[i])) std::swap(data_[m], data_[i]);
                break;
            }
        }
    }

    // вспом-ки
    bool isGrandchild(std::size_t i, std::size_t m) const {
        return m >= left(left(i));
    }

    std::size_t minDescendant(std::size_t i) const {
        return descendantByCompare(i, /*wantMin=*/true);
    }

    std::size_t maxDescendant(std::size_t i) const {
        return descendantByCompare(i, /*wantMin=*/false);
    }

    std::size_t descendantByCompare(std::size_t i, bool wantMin) const {
        std::size_t best = left(i);
        if (right(i) < data_.size() &&
            (wantMin ? cmp_(data_[right(i)], data_[best])
                     : cmp_(data_[best], data_[right(i)])))
            best = right(i);

        // внуки
        for (std::size_t c = left(left(i)); c < data_.size() && c <= right(right(i)); ++c) {
            bool better = wantMin ? cmp_(data_[c], data_[best])
                                  : cmp_(data_[best], data_[c]);
            if (better) best = c;
        }
        return best;
    }

    // удалить узел j: перенести последний элемент на место j и опустить
    void moveLastToRootThenTrickleDown(std::size_t j) {
        data_[j] = std::move(data_.back());
        data_.pop_back();
        if (j < data_.size())
            trickleDown(j);
    }

    // индекс max-элемента среди детей корня
    std::size_t maxIndexUnderRoot() const {
        if (data_.size() == 1) return 0;
        if (data_.size() == 2) return 1;
        return cmp_(data_[1], data_[2]) ? 2 : 1;
    }
};