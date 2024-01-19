#ifndef NOT_IMPL_H_
#define NOT_IMPL_H_

#include <stdexcept>
#include <string>

/**
 * @class NotImplementError - Thrown when a non-implemented method is called
 */
class NotImplementedError : public std::logic_error {
public:
  explicit NotImplementedError(std::string s) : logic_error(s) {}

  explicit NotImplementedError(void) : logic_error("Not Implemented") {}
};

#endif // NOT_IMPL_H_
