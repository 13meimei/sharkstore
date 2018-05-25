package right

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/sessions"
	"github.com/muesli/cache2go"
	"time"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"util/log"
)

var UserCache = cache2go.Cache("user_cache")

const (
	CLUSTER_USER = 0x4
	CLUSTER_ADMIN = 0x2
	SYSTEM_ADMIN = 0x1
)
type Right int16
type User struct {
	Name string
	Right map[int64]Right // clusterid: right
}

func NewUser(name string) *User{
	return &User{
		Name: name,
		Right: make(map[int64]Right),
	}
}

func (user *User) addClusterRight(id, right int64) {
	user.Right[id] = Right(right)
}

func AddCacheUser(user *User) {
	UserCache.Add(user.Name, 5*time.Minute, user)
}

func DelCacheUser(userName string) {
	UserCache.Delete(userName)
}

func GetCacheUser(userName string) *User {
	res, err := UserCache.Value(userName)
	if err != nil {
		log.Error("user [%v] not cached", userName)
		return nil
	}
	return res.Data().(*User)
}

func GetPrivilege(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer c.Next()

		userName, ok := sessions.Default(c).Get("user_name").(string)
		if !ok {
			log.Error("user type assert failed")
			return
		}
		log.Debug("user [%v] get privilege", userName)

		if _, err := GetUserClusterFake(db, userName); err != nil {
			log.Error("user_name [%v] is not exist: %v", userName, err)
			return
		}
	}
}

func GetUserCluster(db *sql.DB, userName string) (*User, error) {
	if user := GetCacheUser(userName); user != nil {
		return user, nil
	}

	req := fmt.Sprintf(`select cluster_id, privilege from fbase_privilege where user_name="%s"`, userName)
	log.Debug("get user req: %v", req)
	rows, err := db.Query(req)
	if err != nil {
		return nil, fmt.Errorf("db query: %v", err)
	}
	//defer db.Close()

	user := NewUser(userName)
	var (
		id int64
		right int64
	)
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &right)
		if err != nil {
			return nil, fmt.Errorf("row error: %v", err)
		}
		user.addClusterRight(id, right)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %v", err)
	}
	if len(user.Right) == 0 {
		return nil, fmt.Errorf("there is no cluster of the user")
	}

	AddCacheUser(user)
	return user, nil
}

func GetUserClusterFake(db *sql.DB, userName string) (*User, error) {
	user := &User{
		Name: userName,
		Right: map[int64]Right{1:1},
	}
	AddCacheUser(user)
	return user, nil
}
