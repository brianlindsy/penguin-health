import { createContext, useContext, useState, useEffect, useCallback } from 'react'
import {
  CognitoUserPool,
  CognitoUser,
  AuthenticationDetails,
} from 'amazon-cognito-identity-js'

const AuthContext = createContext(null)

const userPool = new CognitoUserPool({
  UserPoolId: import.meta.env.VITE_COGNITO_USER_POOL_ID,
  ClientId: import.meta.env.VITE_COGNITO_CLIENT_ID,
})

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const cognitoUser = userPool.getCurrentUser()
    if (cognitoUser) {
      cognitoUser.getSession((err, session) => {
        if (err || !session?.isValid()) {
          setUser(null)
        } else {
          setUser({
            email: cognitoUser.getUsername(),
            token: session.getAccessToken().getJwtToken(),
          })
        }
        setLoading(false)
      })
    } else {
      setLoading(false)
    }
  }, [])

  const login = useCallback((email, password) => {
    return new Promise((resolve, reject) => {
      const cognitoUser = new CognitoUser({
        Username: email,
        Pool: userPool,
      })

      const authDetails = new AuthenticationDetails({
        Username: email,
        Password: password,
      })

      cognitoUser.authenticateUser(authDetails, {
        onSuccess: (session) => {
          setUser({
            email,
            token: session.getAccessToken().getJwtToken(),
          })
          resolve(session)
        },
        onFailure: (err) => {
          reject(err)
        },
        newPasswordRequired: (userAttributes) => {
          // Handle first-time login password change
          resolve({ newPasswordRequired: true, cognitoUser, userAttributes })
        },
      })
    })
  }, [])

  const completeNewPassword = useCallback((cognitoUser, newPassword) => {
    return new Promise((resolve, reject) => {
      cognitoUser.completeNewPasswordChallenge(newPassword, {}, {
        onSuccess: (session) => {
          setUser({
            email: cognitoUser.getUsername(),
            token: session.getAccessToken().getJwtToken(),
          })
          resolve(session)
        },
        onFailure: reject,
      })
    })
  }, [])

  const logout = useCallback(() => {
    const cognitoUser = userPool.getCurrentUser()
    if (cognitoUser) {
      cognitoUser.signOut()
    }
    setUser(null)
  }, [])

  const getToken = useCallback(() => {
    return new Promise((resolve, reject) => {
      const cognitoUser = userPool.getCurrentUser()
      if (!cognitoUser) return reject(new Error('No user'))

      cognitoUser.getSession((err, session) => {
        if (err || !session?.isValid()) {
          setUser(null)
          return reject(err || new Error('Invalid session'))
        }
        const token = session.getAccessToken().getJwtToken()
        setUser(prev => prev ? { ...prev, token } : null)
        resolve(token)
      })
    })
  }, [])

  return (
    <AuthContext.Provider value={{ user, loading, login, logout, getToken, completeNewPassword }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
