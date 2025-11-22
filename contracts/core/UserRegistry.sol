// SPDX-License-Identifier: MIT
pragma solidity ^0.8.18;

import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/access/AccessControlUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";

contract UserRegistry is Initializable, AccessControlUpgradeable, UUPSUpgradeable {
    bytes32 public constant UPGRADER_ROLE = keccak256("UPGRADER_ROLE");

    struct User {
        string profileCid;
        bool exists;
    }

    mapping(address => User) private users;
    address[] private allUsers;

    event UserRegistered(address indexed user, string profileCid);
    event UserUpdated(address indexed user, string profileCid);

    function initialize(address admin,address upgrader) public initializer {
        __AccessControl_init();
        __UUPSUpgradeable_init();
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(UPGRADER_ROLE, upgrader);
    }

    function _authorizeUpgrade(address) internal override onlyRole(UPGRADER_ROLE) {}

    function register(string calldata profileCid) external {
        if(!users[msg.sender].exists){
            users[msg.sender] = User( profileCid, true);
            allUsers.push(msg.sender);
            emit UserRegistered(msg.sender,profileCid);
        }
        
    }

    function update(address user,string calldata profileCid) external onlyRole(DEFAULT_ADMIN_ROLE){
        require(users[user].exists, "not registered");
        users[user].profileCid = profileCid;
        emit UserUpdated(user, profileCid);
    }

    function getUser(address user) external view returns (string memory profileCid) {
        require(users[user].exists, "not registered");
        User storage u = users[user];
        return ( u.profileCid);
    }

    function getUsers(address[] calldata addrs) external view returns ( string[] memory profileCids) {
        uint256 len = addrs.length;
        profileCids = new string[](len);
        for (uint256 i = 0; i < len; i++) {
            User storage u = users[addrs[i]];
            require(u.exists, "user not registered");
            profileCids[i] = u.profileCid;
        }
    }

    function totalUsers() external view returns (uint256) {
        return allUsers.length;
    }
    function isRegistered(address addr) external view returns (bool) {
        return users[addr].exists;
    }
}
