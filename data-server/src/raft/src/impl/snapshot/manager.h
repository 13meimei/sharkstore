_Pragma("once");

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

class Manager final {
public:
    Manager(const SnapshotOptions& opt);
    ~Manager();

    Manager(const Manager&) = delete;
    Manager& operator=(const Manager&) = delete;

private:
    const SnapshotOptions opt_;
};

} /* snapshot  */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
