package main

//Follower map: maps from followee_id -> (follower_id -> true)
//Not threadsafe
var followers = make(map[int](map[int]bool))

//Have fromUserId follow toUserId
func FollowUser(fromUserId int, toUserId int) {
	if followers[toUserId] == nil {
		followers[toUserId] = make(map[int]bool)
	}
	followers[toUserId][fromUserId] = true
}

//Delete follower fromUserId of toUserId
func UnfollowUser(fromUserId int, toUserId int) {
	if followers[toUserId] != nil {
		delete(followers[toUserId], fromUserId)
	}
}

//Get followers of toUserId
func GetFollowers(userId int) []int {
	usersFollowers := make([]int, 0, len(followers[userId]))
	for follower, _ := range followers[userId] {
		usersFollowers = append(usersFollowers, follower)
	}
	return usersFollowers
}
