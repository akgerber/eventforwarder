package main

//Not threadsafe

//Have fromUserId follow toUserId
func FollowUser(s *Service, fromUserId int, toUserId int) {
	if s.followers[toUserId] == nil {
		s.followers[toUserId] = make(map[int]bool)
	}
	s.followers[toUserId][fromUserId] = true
}

//Delete follower fromUserId of toUserId
func UnfollowUser(s *Service, fromUserId int, toUserId int) {
	if s.followers[toUserId] != nil {
		delete(s.followers[toUserId], fromUserId)
	}
}

//Get followers of toUserId
func GetFollowers(s *Service, userId int) []int {
	usersFollowers := make([]int, 0, len(s.followers[userId]))
	for follower, _ := range s.followers[userId] {
		usersFollowers = append(usersFollowers, follower)
	}
	return usersFollowers
}
