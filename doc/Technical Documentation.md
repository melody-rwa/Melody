# Contract Technical Documentation

🔹 **Overview:**
1.  Users and Artists complete registration via **UserRegistry / ArtistRegistry**.
2.  Artists use an invitation code via **MusicFactory** to create a Music Series, which internally deploys **MusicSeries** and **MemeToken**.
3.  Investors purchase **MemeToken** to invest in songs by calling **MusicSeries** to subscribe and record contributions.
4.  Once song financing is complete, **MusicSeries** calls **LPManagerV3** to add liquidity, mint a V3 NFT, and hand it over to **FractionalVaultV3** for management.
5.  **FractionalVaultV3** allocates LP shares to investors and the artist, while transferring the artist's share to **VestingVault** for vesting/locking.
6.  Users can claim fees and redeem LP in **FractionalVaultV3**; Artists claim unlocked earnings in **VestingVault**.
7.  The frontend can call query methods on each contract to retrieve real-time status.

---

## 1. UserRegistry

### Function
User registration and management; stores user information URL addresses.

### Methods

#### `initialize(address admin)`
- **Function**: Initializes the contract and sets the admin.
- **Parameters**:
  - `admin` (address): Administrator address.
- **Returns**: None.

#### `register(string profileCid)`
- **Function**: Registers a user; records the user information URL upon first registration.
- **Parameters**:
  - `profileCid` (string): URL address for user avatar or profile information.
- **Returns**: None.

#### `update(string profileCid)`
- **Function**: Updates the user URL address. Only the admin can modify this.
- **Parameters**:
  - `profileCid` (string): New URL address for avatar/profile information.
- **Returns**: None.

#### `getUser(address user)`
- **Function**: Retrieves information for a specific user.
- **Parameters**:
  - `user` (address): User address.
- **Returns**:
  - `profileCid` (string): URL address for user avatar/profile information.

#### `getUsers(address[] addrs)`
- **Function**: Batch retrieves user information.
- **Parameters**:
  - `addrs` (address[]): Array of user addresses.
- **Returns**:
  - `profileCids` (string[]): Array of URL addresses for user avatar/profile information.

#### `totalUsers()`
- **Function**: Retrieves the total number of registered users.
- **Parameters**: None.
- **Returns**:
  - `count` (uint256): Number of users.

#### `isRegistered(address addr)`
- **Function**: Checks if an address is already registered.
- **Parameters**:
  - `addr` (address): User address.
- **Returns**:
  - `exists` (bool): Whether the user is registered.

### Events

#### `UserRegistered(address user, string profileCid)`
- **Trigger**: Triggered upon first user registration.
- **Parameters**:
  - `user` (address): User address.
  - `profileCid` (string): User profile CID.

#### `UserUpdated(address user, string profileCid)`
- **Trigger**: Triggered when user profile is updated.
- **Parameters**:
  - `user` (address): User address.
  - `profileCid` (string): New profile CID.

---

## 2. ArtistRegistry

### Function
Artist registration and management; stores profile information URLs and levels.

### Methods

#### `initialize(address admin)`
- **Function**: Initializes the contract and sets the admin.
- **Parameters**:
  - `admin` (address): Administrator address.
- **Returns**: None.

#### `register(string profileCid)`
- **Function**: Registers an artist.
- **Parameters**:
  - `profileCid` (string): Artist profile URL address.
- **Returns**: None.

#### `update(string profileCid)`
- **Function**: Updates artist profile. Admin permission required.
- **Parameters**:
  - `profileCid` (string): New profile URL address.
- **Returns**: None.

#### `updateLevel(address artist, uint8 level)`
- **Function**: Updates artist level (only callable by admin).
- **Parameters**:
  - `artist` (address): Artist address.
  - `level` (uint8): New level.
- **Returns**: None.

#### `isArtist(address artist)`
- **Function**: Checks if an address is a registered artist.
- **Parameters**:
  - `artist` (address): Artist address.
- **Returns**:
  - `exists` (bool): Whether the artist is registered.

#### `getArtist(address artist)`
- **Function**: Retrieves artist information.
- **Parameters**:
  - `artist` (address): Artist address.
- **Returns**:
  - `profileCid` (string): Artist profile CID.

#### `getArtists(address[] addrs)`
- **Function**: Batch retrieves artist information.
- **Parameters**:
  - `addrs` (address[]): Array of artist addresses.
- **Returns**:
  - `profileCids` (string[]): Array of artist profile CIDs.

#### `totalArtists()`
- **Function**: Retrieves the total number of artists.
- **Parameters**: None.
- **Returns**:
  - `count` (uint256): Number of artists.

### Events

#### `ArtistRegistered(address artist, string profileCid)`
- **Trigger**: Triggered upon first artist registration.
- **Parameters**:
  - `artist` (address): Artist address.
  - `profileCid` (string): Artist profile CID.

#### `ArtistUpdated(address artist, string profileCid)`
- **Trigger**: Triggered when artist profile is updated.
- **Parameters**:
  - `artist` (address): Artist address.
  - `profileCid` (string): New profile CID.

#### `ArtistLevelUpdated(address artist, uint8 level)`
- **Trigger**: Triggered when an artist's level is updated by the admin.
- **Parameters**:
  - `artist` (address): Artist address.
  - `level` (uint8): New level.

---

## 3. MusicFactory

### Function
Music Factory; responsible for invitation codes, creation of Music Series, and whitelist target management.

### Methods

#### `initialize(address admin, address lpManager, address userRegistry, address artistRegistry, address fractionalVault, address wbnb)`
- **Function**: Initializes the contract.
- **Parameters**:
  - `admin` (address): Administrator.
  - `lpManager` (address): LPManagerV3 address.
  - `userRegistry` (address): User Registry contract.
  - `artistRegistry` (address): Artist Registry contract.
  - `fractionalVault` (address): Fractional Vault address.
  - `wbnb` (address): WBNB token address.
- **Returns**: None.

#### `addAllowedTarget(uint256 target)`
- **Function**: Adds an allowed funding target amount.
- **Parameters**:
  - `target` (uint256): Funding target.
- **Returns**: None.

#### `removeAllowedTarget(uint256 target)`
- **Function**: Removes an allowed funding target amount.
- **Parameters**:
  - `target` (uint256): Funding target.
- **Returns**: None.

#### `getAllowedTargets()`
- **Function**: Retrieves all allowed funding targets.
- **Parameters**: None.
- **Returns**:
  - `targets` (uint256[]): List of funding targets.

#### `addInvites(string[] codes)`
- **Function**: Adds invitation codes.
- **Parameters**:
  - `codes` (string[]): Array of invitation codes.
- **Returns**: None.

#### `getInvites(address artist)`
- **Function**: Retrieves invitation codes for an artist.
- **Parameters**:
  - `artist` (address): Artist address.
- **Returns**:
  - `codes` (string[]): List of invitation codes.

#### `isInviteValid(string calldata code)`
- **Function**: Validates an invitation code.
- **Parameters**:
  - `code` (string): Invitation code.
- **Returns**:
  - `true` if valid, `false` if invalid.

#### `createMusic(string invite, string ipfsCid, uint256 targetBNBWei, uint256 durationSec, string name, string symbol)`
- **Function**: Creates music (uploads music).
- **Parameters**:
  - `invite` (string): Invitation code.
  - `ipfsCid` (string): Music file info URL/address.
  - `targetBNBWei` (uint256): Funding target.
  - `durationSec` (uint256): Music duration.
  - `name` (string): MemeToken name.
  - `symbol` (string): MemeToken symbol.
- **Returns**:
  - `seriesAddr` (address): Address of the created MusicSeries (the artist's music collection).
  - `tokenId` (uint256): Series ID.
  - `memeToken` (address): MemeToken address.

#### `getSeriesOfArtist(address artist)`
- **Function**: Retrieves the MusicSeries address for an artist.
- **Parameters**:
  - `artist` (address): Artist address.
- **Returns**:
  - `series` (address): MusicSeries address.

#### `getArtists(uint256 offset, uint256 limit)`
- **Function**: Retrieves artists with pagination.
- **Parameters**: Pagination parameters.
- **Returns**:
  - `artists` (address[]): Array of artist addresses.

### Events

#### `AllowedTargetAdded(uint256 target)`
- **Trigger**: Triggered when a funding target is added.
- **Parameters**:
  - `target` (uint256): The added target.

#### `AllowedTargetRemoved(uint256 target)`
- **Trigger**: Triggered when a funding target is removed.
- **Parameters**:
  - `target` (uint256): The removed target.

#### `InviteAdded(address artist, string code)`
- **Trigger**: Triggered when an invitation code is added.
- **Parameters**:
  - `artist` (address): Artist address.
  - `code` (string): Invitation code.

#### `MusicCreated(address series, uint256 tokenId, address memeToken)`
- **Trigger**: Triggered when a Music Series is created.
- **Parameters**:
  - `series` (address): MusicSeries address.
  - `tokenId` (uint256): Series ID.
  - `memeToken` (address): MemeToken address.

### MusicSeries Events
*(Note: These events originate from MusicSeries logic but are listed here for reference or if emitted via factory proxy context)*

#### `SongCreated(address indexed seriesAddr, uint256 indexed songId, string ipfsCid, uint256 targetBNBWei, uint256 durationSec, address memeToken, uint256 time)`
- **Trigger**: Triggered after successfully creating a new song.
- **Parameters**:
  - `seriesAddr` (address): Artist's music collection address.
  - `songId` (uint256): New song ID.
  - `ipfsCid` (string): IPFS CID.
  - `targetBNBWei` (uint256): Funding target.
  - `durationSec` (uint256): Duration in seconds.
  - `memeToken` (address): MemeToken contract address.
  - `time` (uint256): Timestamp.

#### `Subscribed(address indexed seriesAddr, uint256 indexed songId, uint256 subId, address user, uint256 amountBNB, uint256 tokenAmount, uint256 secondsBought, uint256 startSec)`
- **Trigger**: Triggered after a user successfully subscribes.
- **Parameters**:
  - `seriesAddr` (address): Artist's music collection address.
  - `songId` (uint256): Song ID.
  - `subId` (uint256): Subscription record ID.
  - `user` (address): User address.
  - `amountBNB` (uint256): Payment amount.
  - `tokenAmount` (uint256): Quantity of MemeToken obtained.
  - `secondsBought` (uint256): Duration purchased (seconds).
  - `startSec` (uint256): Start second of subscription.

#### `WhitelistAdded(address indexed seriesAddr, uint256 indexed songId, address[] accounts)`
- **Trigger**: Triggered after successfully adding a whitelist for a song.
- **Parameters**:
  - `seriesAddr` (address): Artist's music collection address.
  - `songId` (uint256): Song ID.
  - `accounts` (address[]): Array of accounts added to the whitelist.

#### `Finalized(address indexed seriesAddr, uint256 indexed songId, uint256 tokenId, uint128 liquidity)`
- **Trigger**: Triggered after successful finalization and LP minting by LPManager.
- **Parameters**:
  - `seriesAddr` (address): Artist's music collection address.
  - `songId` (uint256): Song ID.
  - `tokenId` (uint256): Minted LP NFT ID.
  - `liquidity` (uint128): Minted liquidity value.

---

## 4. MusicSeries

### Function
Manages a single music series, including user whitelists, user subscriptions, contribution statistics, and final fundraising.

### Methods

#### `createSong(string ipfsCid, uint256 targetBNBWei, uint256 durationSec, string name, string symbol)`
- **Function**: Creates a new song (calls `MusicFactory.createMusic`).
- **Parameters**:
  - `ipfsCid` (string): Song file CID.
  - `targetBNBWei` (uint256): Funding target.
  - `durationSec` (uint256): Music duration.
  - `name` (string): MemeToken name.
  - `symbol` (string): MemeToken symbol.
- **Returns**:
  - `tokenId` (uint256): Song ID.
  - `memeTokenAddr` (address): MemeToken address.

#### `addWhitelist(uint256 songId, address[] calldata accounts)`
- **Function**: Adds whitelist addresses for a specific `songId` (supports adding multiple accounts).
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `accounts` (address[]): Array of addresses to add to the whitelist.
- **Returns**: None.

#### `subscribe(uint256 songId) payable`
- **Function**: User subscription (pays BNB). The contract allocates MemeToken and seconds based on payment amount and records the contribution.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `msg.value` (uint256): Amount of ETH/BNB paid by the caller.
- **Returns**:
  - `subId` (uint256): ID of this subscription record.
  - `tokenAmount` (uint256): Quantity of MemeToken obtained.
  - `secondsBought` (uint256): Redeemable duration obtained (seconds).

#### `getSubscriptionCount(uint256 songId)`
- **Function**: Retrieves the total number of subscription records for a song (for pagination).
- **Parameters**:
  - `songId` (uint256): Song ID.
- **Returns**:
  - `count` (uint256): Total number of subscription records.

#### `getSubscriptions(uint256 songId, uint256 offset, uint256 limit)`
- **Function**: Retrieves subscription details with pagination.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `offset` (uint256): Start index.
  - `limit` (uint256): Maximum number of records to return.
- **Returns**:
  - `subscriptions` (Subscription[]): Array of subscription records. Fields may include:
    - `id`
    - `user`
    - `amountBNB`
    - `tokenAmount`
    - `secondsBought`
    - `startSec`
    - `timestamp`

#### `getContributions(uint256 songId)`
- **Function**: Retrieves contributions from all users for a song (for LPManager allocation).
- **Parameters**:
  - `songId` (uint256): Song ID.
- **Returns**:
  - `users` (address[]): Array of subscriber addresses.
  - `amounts` (uint256[]): Array of corresponding contribution amounts.

#### `finalize(uint256 songId)`
- **Function**: Completes the fundraising process (upon reaching time or target), triggers LPManager to mint LP, and completes allocation.
- **Parameters**:
  - `songId` (uint256): Song ID.
- **Returns**: None (or emits event, or returns `(tokenId, liquidity)` depending on implementation).

#### `getSongs(uint256 offset, uint256 limit)`
- **Function**: Retrieves basic song information with pagination.
- **Parameters**:
  - `offset` (uint256): Start index.
  - `limit` (uint256): Maximum number of records to return.
- **Returns**:
  - `songs` (SongBasic[]): Array of basic song information. Fields may include:
    - `tokenId`
    - `ipfsCid`
    - `targetBNB`
    - `raisedBNB`
    - `durationSec`
    - `memeToken`
    - `totalMemeSupply`
    - `subscribedTokens`
    - `createdAt`
    - `subCount`
    - `phase`

#### `currentPhase(uint256 songId)`
- **Function**: Queries the current phase of the song (Whitelist, Public, Ended/Finalized, etc.).
- **Parameters**:
  - `songId` (uint256): Song ID.
- **Returns**:
  - `phase` (uint8): Phase number (specific enum values defined by the contract).

### 📌 Events
*(Refer to the Events section listed under MusicFactory above)*

---

## 5. FractionalVaultV3

### Function
Manages LP allocation, user shares, fee claiming, and redemption.

### Methods

#### `initialize(address _positionManager, address _admin, address _platformFeeMultiSig, address _platformMultiSig, address _lpManager, address _vestingVault)`
- **Function**: Initializes the contract, setting `positionManager`, `admin`, platform multisig addresses, `lpManager`, and `vestingVault`.
- **Parameters**:
  - `_positionManager (address)`: `INonfungiblePositionManager` address (used for `collect` / `decreaseLiquidity` / `positions`).
  - `_admin (address)`: Admin address (grants roles).
  - `_platformFeeMultiSig (address)`: Platform 17% locked address (cannot redeem).
  - `_platformMultiSig (address)`: Platform 3% address (redeemable like a normal user).
  - `_lpManager (address)`: Only this address can call `finalizeDistribution`.
  - `_vestingVault (address)`: `VestingVault` address (where the Artist's 30% is stored).
- **Returns**: None.

#### `getPendingForUser(uint256 songId, address user)`
- **Function**: View function; estimates the current claimable fees for a user (includes `tokensOwed` in the position not yet collected).
- **Parameters**:
  - `songId (uint256)`: Song ID.
  - `user (address)`: User address.
- **Returns**:
  - `pending0 (uint256)`: Estimated claimable amount of `token0`.
  - `pending1 (uint256)`: Estimated claimable amount of `token1`.

#### `finalizeDistribution(uint256 songId, uint256 tokenId, address token0, address token1, uint128 liquidity, address[] userAddrs, uint128[] userLiqAmounts, address artist)`
- **Function**: Completes allocation and records distribution information.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `tokenId` (uint256): LP Token ID.
  - `token0` (address): Token0 address.
  - `token1` (address): Token1 address.
  - `liquidity` (uint128): Total liquidity.
  - `userAddrs` (address[]): Investor user addresses.
  - `userLiqAmounts` (uint128[]): Investor user allocation shares.
  - `artist` (address): Artist address.
- **Returns**: None.

#### `claim(uint256 songId)`
- **Function**: User claims fees.
- **Parameters**:
  - `songId` (uint256): Song ID.
- **Returns**:
  - `token0` (address): Token0 address.
  - `token1` (address): Token1 address.
  - `amount0` (uint256): Token0 amount.
  - `amount1` (uint256): Token1 amount.

#### `redeem(uint256 songId, uint256 amount)`
- **Function**: Redeems LP shares.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `amount` (uint256): Amount of LP to redeem.
- **Returns**:
  - `token0` (address): Token0 address.
  - `token1` (address): Token1 address.
  - `amount0` (uint256): Redeemed Token0.
  - `amount1` (uint256): Redeemed Token1.

#### `getDistribution(uint256 songId)`
- **Function**: Retrieves distribution information.
- **Parameters**:
  - `songId` (uint256): Song ID.
- **Returns**:
  - `DistributionInfo`: Distribution details.

#### `getDistributions(uint256[] songIds)`
- **Function**: Batch retrieves distribution information.
- **Parameters**:
  - `songIds` (uint256[]): Array of Song IDs.
- **Returns**:
  - `DistributionInfo[]`: Array of distribution details.

#### `getDistributionsByArtist(address artist)`
- **Function**: Retrieves distributions related to an artist.
- **Parameters**:
  - `artist` (address): Artist address.
- **Returns**:
  - `DistributionInfo[]`: Array of distribution details.

### Events

#### `FinalizedDistribution(uint256 songId, uint256 tokenId, uint128 liquidity, address vestingVault)`
- **Trigger**: Triggered when distribution is finalized.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `tokenId` (uint256): LP Token ID.
  - `liquidity` (uint128): Total liquidity.
  - `vestingVault` (address): Associated VestingVault.

#### `Claimed(uint256 songId, address user, uint256 amount0, uint256 amount1)`
- **Trigger**: Triggered when a user claims fees.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `user` (address): User address.
  - `amount0` (uint256): Token0 amount.
  - `amount1` (uint256): Token1 amount.

#### `Redeemed(uint256 songId, address user, uint256 amount, uint256 amount0, uint256 amount1)`
- **Trigger**: Triggered when a user redeems LP.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `user` (address): User address.
  - `amount` (uint256): Redeemed share.
  - `amount0` (uint256): Token0 amount.
  - `amount1` (uint256): Token1 amount.

---

## 6. VestingVault

### Function
Manages artist lock-ups (vesting) and linear release.

### Methods

#### `initialize(address admin, address fractionalVault)`
- **Function**: Initializes the contract.
- **Parameters**:
  - `admin` (address): Admin address.
  - `fractionalVault` (address): FractionalVault address.
- **Returns**: None.

#### `addVesting(uint256 songId, address artist, uint256 amount, uint256 start, uint256 duration)`
- **Function**: Adds a vesting schedule (called by FractionalVault).
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `artist` (address): Artist address.
  - `amount` (uint256): Total allocated amount.
  - `start` (uint256): Start time.
  - `duration` (uint256): Vesting duration.
- **Returns**: None.

#### `claimFees(uint256 songId)`
- **Function**: Artist claims fees.
- **Parameters**:
  - `songId` (uint256): Song ID.
- **Returns**: None.

#### `redeem(uint256 songId)`
- **Function**: Artist redeems the unlocked portion of the vesting.
- **Parameters**:
  - `songId` (uint256): Song ID.
- **Returns**: None.

#### `vestedAmount(uint256 songId)`
- **Function**: Checks the vested (unlocked) amount for a specific schedule.
- **Parameters**:
  - `songId` (uint256): Song ID.
- **Returns**:
  - `amount` (uint256): Vested amount.

#### `releasableAmount(uint256 songId)`
- **Function**: Checks the claimable (releasable) amount for a specific schedule.
- **Parameters**:
  - `songId` (uint256): Song ID.
- **Returns**:
  - `amount` (uint256): Claimable amount.

#### `setFractionalVault(address newVault)`
- **Function**: Sets a new FractionalVault address.
- **Parameters**:
  - `newVault` (address): New address.
- **Returns**: None.

### Events

#### `VestingAdded(uint256 songId, address artist, uint256 amount, uint256 start, uint256 duration)`
- **Trigger**: Triggered when vesting is added.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `artist` (address): Artist.
  - `amount` (uint256): Total vesting amount.
  - `start` (uint256): Start time.
  - `duration` (uint256): Vesting duration.

#### `FeesClaimed(uint256 songId, address artist, address token0, address token1, uint256 amount0, uint256 amount1)`
- **Trigger**: Triggered when an artist claims fees.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `artist` (address): Artist address.
  - `token0` (address): Token0 address.
  - `token1` (address): Token1 address.
  - `amount0` (uint256): Token0 amount.
  - `amount1` (uint256): Token1 amount.

#### `LPRedeemed(uint256 songId, address artist, uint256 lpUnits, address token0, address token1, uint256 amount0, uint256 amount1)`
- **Trigger**: Triggered when an artist redeems LP.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `artist` (address): Artist address.
  - `lpUnits` (uint256): Redeemed LP quantity.
  - `token0` (address): Token0 address.
  - `token1` (address): Token1 address.
  - `amount0` (uint256): Token0 amount.
  - `amount1` (uint256): Token1 amount.

---

## 7. LPManagerV3

### Function
Interacts with Pancakeswap V3 PositionManager; handles LP minting and allocation.

### Methods

#### `initialize(address pm, address fractionalVault, address admin)`
- **Function**: Initializes the contract.
- **Parameters**:
  - `pm` (address): Pancakeswap V3 PositionManager address.
  - `fractionalVault` (address): FractionalVault address.
  - `admin` (address): Administrator.
- **Returns**: None.

#### `registerSeries(address series)`
- **Function**: Factory registers a new MusicSeries.
- **Parameters**:
  - `series` (address): MusicSeries address.
- **Returns**: None.

#### `addLiquidityAndFinalizeV3(uint256 songId, address token0, address token1, uint24 fee, int24 tickLower, int24 tickUpper, uint256 amount0Desired, uint256 amount1Desired, uint256 amount0Min, uint256 amount1Min, address artist)`
- **Function**: Called by MusicSeries to add liquidity and complete allocation.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `token0` (address): Token0 address.
  - `token1` (address): Token1 address.
  - `fee` (uint24): Fee tier.
  - `tickLower` (int24): Tick lower bound.
  - `tickUpper` (int24): Tick upper bound.
  - `amount0Desired` (uint256): Desired Token0 amount.
  - `amount1Desired` (uint256): Desired Token1 amount.
  - `amount0Min` (uint256): Minimum Token0 amount.
  - `amount1Min` (uint256): Minimum Token1 amount.
  - `artist` (address): Artist address.
- **Returns**:
  - `tokenId` (uint256): NFT Token ID.
  - `liquidity` (uint128): Minted liquidity.

### Events

#### `LiquidityAddedAndFinalized(uint256 songId, uint256 tokenId, uint128 liquidity)`
- **Trigger**: Triggered when liquidity is added and allocation is finalized.
- **Parameters**:
  - `songId` (uint256): Song ID.
  - `tokenId` (uint256): LP Token ID.
  - `liquidity` (uint128): Total liquidity.

---

## 8. MemeToken

### Function
Standard BEP-20 token representing single investment shares.

### Methods

#### `initialize(string name, string symbol, address series, uint256 songId)`
- **Function**: Initializes the token.
- **Parameters**:
  - `name` (string): Token name.
  - `symbol` (string): Token symbol.
  - `series` (address): Belonging MusicSeries.
  - `songId` (uint256): Song ID.
- **Returns**: None.

#### `mint(address to, uint256 amount)`
- **Function**: Mints tokens.
- **Parameters**:
  - `to` (address): Recipient address.
  - `amount` (uint256): Amount to mint.
- **Returns**: None.

#### `burn(address from, uint256 amount)`
- **Function**: Burns tokens.
- **Parameters**:
  - `from` (address): Address to burn from.
  - `amount` (uint256): Amount to burn.
- **Returns**: None.

### Events

#### `Transfer(address from, address to, uint256 value)`
- **Trigger**: Triggered during token transfer.
- **Parameters**:
  - `from` (address): Sender address.
  - `to` (address): Recipient address.
  - `value` (uint256): Transferred amount.

#### `Approval(address owner, address spender, uint256 value)`
- **Trigger**: Triggered during approval.
- **Parameters**:
  - `owner` (address): Owner address.
  - `spender` (address): Spender address.
  - `value` (uint256): Approved amount.

---
