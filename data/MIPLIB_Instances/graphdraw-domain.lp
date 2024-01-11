\ Model graphdraw-domain
\ LP format - for model browsing. Use MPS format to capture full model detail.
Minimize
  center_dist[r,Client] + center_dist[r,CertificateTemplate]
   + center_dist[r,Course] + center_dist[r,Role] + center_dist[r,User]
   + center_dist[r,CourseSession] + center_dist[r,Clinic]
   + center_dist[r,Location] + center_dist[r,Therapist]
   + center_dist[r,Instructor] + center_dist[c,Client]
   + center_dist[c,CertificateTemplate] + center_dist[c,Course]
   + center_dist[c,Role] + center_dist[c,User]
   + center_dist[c,CourseSession] + center_dist[c,Clinic]
   + center_dist[c,Location] + center_dist[c,Therapist]
   + center_dist[c,Instructor] + 86 dist[r,Client,CertificateTemplate]
   + 86 dist[r,Client,CourseSession] + 86 dist[r,Client,Instructor]
   + 86 dist[r,Client,Location] + 86 dist[r,Client,User]
   + 86 dist[r,CertificateTemplate,CourseSession]
   + 86 dist[r,Course,CourseSession] + 86 dist[r,CourseSession,Instructor]
   + 86 dist[r,Instructor,Location] + 86 dist[r,Clinic,Location]
   + 86 dist[r,Clinic,Therapist] + 86 dist[r,Location,Therapist]
   + 86 dist[r,Location,CourseSession] + 86 dist[r,Therapist,CourseSession]
   + 86 dist[r,Role,User] + 86 dist[r,User,Instructor]
   + 86 dist[r,User,Therapist] + 86 dist[c,Client,CertificateTemplate]
   + 86 dist[c,Client,CourseSession] + 86 dist[c,Client,Instructor]
   + 86 dist[c,Client,Location] + 86 dist[c,Client,User]
   + 86 dist[c,CertificateTemplate,CourseSession]
   + 86 dist[c,Course,CourseSession] + 86 dist[c,CourseSession,Instructor]
   + 86 dist[c,Instructor,Location] + 86 dist[c,Clinic,Location]
   + 86 dist[c,Clinic,Therapist] + 86 dist[c,Location,Therapist]
   + 86 dist[c,Location,CourseSession] + 86 dist[c,Therapist,CourseSession]
   + 86 dist[c,Role,User] + 86 dist[c,User,Instructor]
   + 86 dist[c,User,Therapist]
Subject To
 choose1[Client,CertificateTemplate]: zV[r,rt,Client,CertificateTemplate]
   + zV[c,rt,Client,CertificateTemplate]
   + zV[r,lb,Client,CertificateTemplate]
   + zV[c,lb,Client,CertificateTemplate] = 1
 choose1[Client,Course]: zV[r,rt,Client,Course] + zV[c,rt,Client,Course]
   + zV[r,lb,Client,Course] + zV[c,lb,Client,Course] = 1
 choose1[Client,Role]: zV[r,rt,Client,Role] + zV[c,rt,Client,Role]
   + zV[r,lb,Client,Role] + zV[c,lb,Client,Role] = 1
 choose1[Client,User]: zV[r,rt,Client,User] + zV[c,rt,Client,User]
   + zV[r,lb,Client,User] + zV[c,lb,Client,User] = 1
 choose1[Client,CourseSession]: zV[r,rt,Client,CourseSession]
   + zV[c,rt,Client,CourseSession] + zV[r,lb,Client,CourseSession]
   + zV[c,lb,Client,CourseSession] = 1
 choose1[Client,Clinic]: zV[r,rt,Client,Clinic] + zV[c,rt,Client,Clinic]
   + zV[r,lb,Client,Clinic] + zV[c,lb,Client,Clinic] = 1
 choose1[Client,Location]: zV[r,rt,Client,Location]
   + zV[c,rt,Client,Location] + zV[r,lb,Client,Location]
   + zV[c,lb,Client,Location] = 1
 choose1[Client,Therapist]: zV[r,rt,Client,Therapist]
   + zV[c,rt,Client,Therapist] + zV[r,lb,Client,Therapist]
   + zV[c,lb,Client,Therapist] = 1
 choose1[Client,Instructor]: zV[r,rt,Client,Instructor]
   + zV[c,rt,Client,Instructor] + zV[r,lb,Client,Instructor]
   + zV[c,lb,Client,Instructor] = 1
 choose1[CertificateTemplate,Course]: zV[r,rt,CertificateTemplate,Course]
   + zV[c,rt,CertificateTemplate,Course]
   + zV[r,lb,CertificateTemplate,Course]
   + zV[c,lb,CertificateTemplate,Course] = 1
 choose1[CertificateTemplate,Role]: zV[r,rt,CertificateTemplate,Role]
   + zV[c,rt,CertificateTemplate,Role] + zV[r,lb,CertificateTemplate,Role]
   + zV[c,lb,CertificateTemplate,Role] = 1
 choose1[CertificateTemplate,User]: zV[r,rt,CertificateTemplate,User]
   + zV[c,rt,CertificateTemplate,User] + zV[r,lb,CertificateTemplate,User]
   + zV[c,lb,CertificateTemplate,User] = 1
 choose1[CertificateTemplate,CourseSession]:
   zV[r,rt,CertificateTemplate,CourseSession]
   + zV[c,rt,CertificateTemplate,CourseSession]
   + zV[r,lb,CertificateTemplate,CourseSession]
   + zV[c,lb,CertificateTemplate,CourseSession] = 1
 choose1[CertificateTemplate,Clinic]: zV[r,rt,CertificateTemplate,Clinic]
   + zV[c,rt,CertificateTemplate,Clinic]
   + zV[r,lb,CertificateTemplate,Clinic]
   + zV[c,lb,CertificateTemplate,Clinic] = 1
 choose1[CertificateTemplate,Location]:
   zV[r,rt,CertificateTemplate,Location]
   + zV[c,rt,CertificateTemplate,Location]
   + zV[r,lb,CertificateTemplate,Location]
   + zV[c,lb,CertificateTemplate,Location] = 1
 choose1[CertificateTemplate,Therapist]:
   zV[r,rt,CertificateTemplate,Therapist]
   + zV[c,rt,CertificateTemplate,Therapist]
   + zV[r,lb,CertificateTemplate,Therapist]
   + zV[c,lb,CertificateTemplate,Therapist] = 1
 choose1[CertificateTemplate,Instructor]:
   zV[r,rt,CertificateTemplate,Instructor]
   + zV[c,rt,CertificateTemplate,Instructor]
   + zV[r,lb,CertificateTemplate,Instructor]
   + zV[c,lb,CertificateTemplate,Instructor] = 1
 choose1[Course,Role]: zV[r,rt,Course,Role] + zV[c,rt,Course,Role]
   + zV[r,lb,Course,Role] + zV[c,lb,Course,Role] = 1
 choose1[Course,User]: zV[r,rt,Course,User] + zV[c,rt,Course,User]
   + zV[r,lb,Course,User] + zV[c,lb,Course,User] = 1
 choose1[Course,CourseSession]: zV[r,rt,Course,CourseSession]
   + zV[c,rt,Course,CourseSession] + zV[r,lb,Course,CourseSession]
   + zV[c,lb,Course,CourseSession] = 1
 choose1[Course,Clinic]: zV[r,rt,Course,Clinic] + zV[c,rt,Course,Clinic]
   + zV[r,lb,Course,Clinic] + zV[c,lb,Course,Clinic] = 1
 choose1[Course,Location]: zV[r,rt,Course,Location]
   + zV[c,rt,Course,Location] + zV[r,lb,Course,Location]
   + zV[c,lb,Course,Location] = 1
 choose1[Course,Therapist]: zV[r,rt,Course,Therapist]
   + zV[c,rt,Course,Therapist] + zV[r,lb,Course,Therapist]
   + zV[c,lb,Course,Therapist] = 1
 choose1[Course,Instructor]: zV[r,rt,Course,Instructor]
   + zV[c,rt,Course,Instructor] + zV[r,lb,Course,Instructor]
   + zV[c,lb,Course,Instructor] = 1
 choose1[Role,User]: zV[r,rt,Role,User] + zV[c,rt,Role,User]
   + zV[r,lb,Role,User] + zV[c,lb,Role,User] = 1
 choose1[Role,CourseSession]: zV[r,rt,Role,CourseSession]
   + zV[c,rt,Role,CourseSession] + zV[r,lb,Role,CourseSession]
   + zV[c,lb,Role,CourseSession] = 1
 choose1[Role,Clinic]: zV[r,rt,Role,Clinic] + zV[c,rt,Role,Clinic]
   + zV[r,lb,Role,Clinic] + zV[c,lb,Role,Clinic] = 1
 choose1[Role,Location]: zV[r,rt,Role,Location] + zV[c,rt,Role,Location]
   + zV[r,lb,Role,Location] + zV[c,lb,Role,Location] = 1
 choose1[Role,Therapist]: zV[r,rt,Role,Therapist] + zV[c,rt,Role,Therapist]
   + zV[r,lb,Role,Therapist] + zV[c,lb,Role,Therapist] = 1
 choose1[Role,Instructor]: zV[r,rt,Role,Instructor]
   + zV[c,rt,Role,Instructor] + zV[r,lb,Role,Instructor]
   + zV[c,lb,Role,Instructor] = 1
 choose1[User,CourseSession]: zV[r,rt,User,CourseSession]
   + zV[c,rt,User,CourseSession] + zV[r,lb,User,CourseSession]
   + zV[c,lb,User,CourseSession] = 1
 choose1[User,Clinic]: zV[r,rt,User,Clinic] + zV[c,rt,User,Clinic]
   + zV[r,lb,User,Clinic] + zV[c,lb,User,Clinic] = 1
 choose1[User,Location]: zV[r,rt,User,Location] + zV[c,rt,User,Location]
   + zV[r,lb,User,Location] + zV[c,lb,User,Location] = 1
 choose1[User,Therapist]: zV[r,rt,User,Therapist] + zV[c,rt,User,Therapist]
   + zV[r,lb,User,Therapist] + zV[c,lb,User,Therapist] = 1
 choose1[User,Instructor]: zV[r,rt,User,Instructor]
   + zV[c,rt,User,Instructor] + zV[r,lb,User,Instructor]
   + zV[c,lb,User,Instructor] = 1
 choose1[CourseSession,Clinic]: zV[r,rt,CourseSession,Clinic]
   + zV[c,rt,CourseSession,Clinic] + zV[r,lb,CourseSession,Clinic]
   + zV[c,lb,CourseSession,Clinic] = 1
 choose1[CourseSession,Location]: zV[r,rt,CourseSession,Location]
   + zV[c,rt,CourseSession,Location] + zV[r,lb,CourseSession,Location]
   + zV[c,lb,CourseSession,Location] = 1
 choose1[CourseSession,Therapist]: zV[r,rt,CourseSession,Therapist]
   + zV[c,rt,CourseSession,Therapist] + zV[r,lb,CourseSession,Therapist]
   + zV[c,lb,CourseSession,Therapist] = 1
 choose1[CourseSession,Instructor]: zV[r,rt,CourseSession,Instructor]
   + zV[c,rt,CourseSession,Instructor] + zV[r,lb,CourseSession,Instructor]
   + zV[c,lb,CourseSession,Instructor] = 1
 choose1[Clinic,Location]: zV[r,rt,Clinic,Location]
   + zV[c,rt,Clinic,Location] + zV[r,lb,Clinic,Location]
   + zV[c,lb,Clinic,Location] = 1
 choose1[Clinic,Therapist]: zV[r,rt,Clinic,Therapist]
   + zV[c,rt,Clinic,Therapist] + zV[r,lb,Clinic,Therapist]
   + zV[c,lb,Clinic,Therapist] = 1
 choose1[Clinic,Instructor]: zV[r,rt,Clinic,Instructor]
   + zV[c,rt,Clinic,Instructor] + zV[r,lb,Clinic,Instructor]
   + zV[c,lb,Clinic,Instructor] = 1
 choose1[Location,Therapist]: zV[r,rt,Location,Therapist]
   + zV[c,rt,Location,Therapist] + zV[r,lb,Location,Therapist]
   + zV[c,lb,Location,Therapist] = 1
 choose1[Location,Instructor]: zV[r,rt,Location,Instructor]
   + zV[c,rt,Location,Instructor] + zV[r,lb,Location,Instructor]
   + zV[c,lb,Location,Instructor] = 1
 choose1[Therapist,Instructor]: zV[r,rt,Therapist,Instructor]
   + zV[c,rt,Therapist,Instructor] + zV[r,lb,Therapist,Instructor]
   + zV[c,lb,Therapist,Instructor] = 1
 centerDistAxis1[r,Client]: x[r,Client] + center_dist[r,Client] >= 17.5
 centerDistAxis1[r,CertificateTemplate]: x[r,CertificateTemplate]
   + center_dist[r,CertificateTemplate] >= 18.5
 centerDistAxis1[r,Course]: x[r,Course] + center_dist[r,Course] >= 19
 centerDistAxis1[r,Role]: x[r,Role] + center_dist[r,Role] >= 19
 centerDistAxis1[r,User]: x[r,User] + center_dist[r,User] >= 18.5
 centerDistAxis1[r,CourseSession]: x[r,CourseSession]
   + center_dist[r,CourseSession] >= 17.5
 centerDistAxis1[r,Clinic]: x[r,Clinic] + center_dist[r,Clinic] >= 18
 centerDistAxis1[r,Location]: x[r,Location] + center_dist[r,Location]
   >= 17.5
 centerDistAxis1[r,Therapist]: x[r,Therapist] + center_dist[r,Therapist]
   >= 17
 centerDistAxis1[r,Instructor]: x[r,Instructor] + center_dist[r,Instructor]
   >= 17
 centerDistAxis1[c,Client]: x[c,Client] + center_dist[c,Client] >= 16.5
 centerDistAxis1[c,CertificateTemplate]: x[c,CertificateTemplate]
   + center_dist[c,CertificateTemplate] >= 16
 centerDistAxis1[c,Course]: x[c,Course] + center_dist[c,Course] >= 17
 centerDistAxis1[c,Role]: x[c,Role] + center_dist[c,Role] >= 17
 centerDistAxis1[c,User]: x[c,User] + center_dist[c,User] >= 16.5
 centerDistAxis1[c,CourseSession]: x[c,CourseSession]
   + center_dist[c,CourseSession] >= 15
 centerDistAxis1[c,Clinic]: x[c,Clinic] + center_dist[c,Clinic] >= 16.5
 centerDistAxis1[c,Location]: x[c,Location] + center_dist[c,Location]
   >= 16.5
 centerDistAxis1[c,Therapist]: x[c,Therapist] + center_dist[c,Therapist]
   >= 16.5
 centerDistAxis1[c,Instructor]: x[c,Instructor] + center_dist[c,Instructor]
   >= 16
 centerDistAxis2[r,Client]: - x[r,Client] + center_dist[r,Client] >= -17.5
 centerDistAxis2[r,CertificateTemplate]: - x[r,CertificateTemplate]
   + center_dist[r,CertificateTemplate] >= -18.5
 centerDistAxis2[r,Course]: - x[r,Course] + center_dist[r,Course] >= -19
 centerDistAxis2[r,Role]: - x[r,Role] + center_dist[r,Role] >= -19
 centerDistAxis2[r,User]: - x[r,User] + center_dist[r,User] >= -18.5
 centerDistAxis2[r,CourseSession]: - x[r,CourseSession]
   + center_dist[r,CourseSession] >= -17.5
 centerDistAxis2[r,Clinic]: - x[r,Clinic] + center_dist[r,Clinic] >= -18
 centerDistAxis2[r,Location]: - x[r,Location] + center_dist[r,Location]
   >= -17.5
 centerDistAxis2[r,Therapist]: - x[r,Therapist] + center_dist[r,Therapist]
   >= -17
 centerDistAxis2[r,Instructor]: - x[r,Instructor]
   + center_dist[r,Instructor] >= -17
 centerDistAxis2[c,Client]: - x[c,Client] + center_dist[c,Client] >= -16.5
 centerDistAxis2[c,CertificateTemplate]: - x[c,CertificateTemplate]
   + center_dist[c,CertificateTemplate] >= -16
 centerDistAxis2[c,Course]: - x[c,Course] + center_dist[c,Course] >= -17
 centerDistAxis2[c,Role]: - x[c,Role] + center_dist[c,Role] >= -17
 centerDistAxis2[c,User]: - x[c,User] + center_dist[c,User] >= -16.5
 centerDistAxis2[c,CourseSession]: - x[c,CourseSession]
   + center_dist[c,CourseSession] >= -15
 centerDistAxis2[c,Clinic]: - x[c,Clinic] + center_dist[c,Clinic] >= -16.5
 centerDistAxis2[c,Location]: - x[c,Location] + center_dist[c,Location]
   >= -16.5
 centerDistAxis2[c,Therapist]: - x[c,Therapist] + center_dist[c,Therapist]
   >= -16.5
 centerDistAxis2[c,Instructor]: - x[c,Instructor]
   + center_dist[c,Instructor] >= -16
 axisVdifLB[r,Client,CertificateTemplate]: - x[r,Client]
   + x[r,CertificateTemplate] - 51 zV[r,lb,Client,CertificateTemplate]
   >= -42
 axisVdifLB[r,Client,Course]: - x[r,Client] + x[r,Course]
   - 51 zV[r,lb,Client,Course] >= -42
 axisVdifLB[r,Client,Role]: - x[r,Client] + x[r,Role]
   - 51 zV[r,lb,Client,Role] >= -42
 axisVdifLB[r,Client,User]: - x[r,Client] + x[r,User]
   - 51 zV[r,lb,Client,User] >= -42
 axisVdifLB[r,Client,CourseSession]: - x[r,Client] + x[r,CourseSession]
   - 51 zV[r,lb,Client,CourseSession] >= -42
 axisVdifLB[r,Client,Clinic]: - x[r,Client] + x[r,Clinic]
   - 51 zV[r,lb,Client,Clinic] >= -42
 axisVdifLB[r,Client,Location]: - x[r,Client] + x[r,Location]
   - 51 zV[r,lb,Client,Location] >= -42
 axisVdifLB[r,Client,Therapist]: - x[r,Client] + x[r,Therapist]
   - 51 zV[r,lb,Client,Therapist] >= -42
 axisVdifLB[r,Client,Instructor]: - x[r,Client] + x[r,Instructor]
   - 51 zV[r,lb,Client,Instructor] >= -42
 axisVdifLB[r,CertificateTemplate,Course]: - x[r,CertificateTemplate]
   + x[r,Course] - 49 zV[r,lb,CertificateTemplate,Course] >= -42
 axisVdifLB[r,CertificateTemplate,Role]: - x[r,CertificateTemplate]
   + x[r,Role] - 49 zV[r,lb,CertificateTemplate,Role] >= -42
 axisVdifLB[r,CertificateTemplate,User]: - x[r,CertificateTemplate]
   + x[r,User] - 49 zV[r,lb,CertificateTemplate,User] >= -42
 axisVdifLB[r,CertificateTemplate,CourseSession]:
   - x[r,CertificateTemplate] + x[r,CourseSession]
   - 49 zV[r,lb,CertificateTemplate,CourseSession] >= -42
 axisVdifLB[r,CertificateTemplate,Clinic]: - x[r,CertificateTemplate]
   + x[r,Clinic] - 49 zV[r,lb,CertificateTemplate,Clinic] >= -42
 axisVdifLB[r,CertificateTemplate,Location]: - x[r,CertificateTemplate]
   + x[r,Location] - 49 zV[r,lb,CertificateTemplate,Location] >= -42
 axisVdifLB[r,CertificateTemplate,Therapist]: - x[r,CertificateTemplate]
   + x[r,Therapist] - 49 zV[r,lb,CertificateTemplate,Therapist] >= -42
 axisVdifLB[r,CertificateTemplate,Instructor]: - x[r,CertificateTemplate]
   + x[r,Instructor] - 49 zV[r,lb,CertificateTemplate,Instructor] >= -42
 axisVdifLB[r,Course,Role]: - x[r,Course] + x[r,Role]
   - 48 zV[r,lb,Course,Role] >= -42
 axisVdifLB[r,Course,User]: - x[r,Course] + x[r,User]
   - 48 zV[r,lb,Course,User] >= -42
 axisVdifLB[r,Course,CourseSession]: - x[r,Course] + x[r,CourseSession]
   - 48 zV[r,lb,Course,CourseSession] >= -42
 axisVdifLB[r,Course,Clinic]: - x[r,Course] + x[r,Clinic]
   - 48 zV[r,lb,Course,Clinic] >= -42
 axisVdifLB[r,Course,Location]: - x[r,Course] + x[r,Location]
   - 48 zV[r,lb,Course,Location] >= -42
 axisVdifLB[r,Course,Therapist]: - x[r,Course] + x[r,Therapist]
   - 48 zV[r,lb,Course,Therapist] >= -42
 axisVdifLB[r,Course,Instructor]: - x[r,Course] + x[r,Instructor]
   - 48 zV[r,lb,Course,Instructor] >= -42
 axisVdifLB[r,Role,User]: - x[r,Role] + x[r,User] - 48 zV[r,lb,Role,User]
   >= -42
 axisVdifLB[r,Role,CourseSession]: - x[r,Role] + x[r,CourseSession]
   - 48 zV[r,lb,Role,CourseSession] >= -42
 axisVdifLB[r,Role,Clinic]: - x[r,Role] + x[r,Clinic]
   - 48 zV[r,lb,Role,Clinic] >= -42
 axisVdifLB[r,Role,Location]: - x[r,Role] + x[r,Location]
   - 48 zV[r,lb,Role,Location] >= -42
 axisVdifLB[r,Role,Therapist]: - x[r,Role] + x[r,Therapist]
   - 48 zV[r,lb,Role,Therapist] >= -42
 axisVdifLB[r,Role,Instructor]: - x[r,Role] + x[r,Instructor]
   - 48 zV[r,lb,Role,Instructor] >= -42
 axisVdifLB[r,User,CourseSession]: - x[r,User] + x[r,CourseSession]
   - 49 zV[r,lb,User,CourseSession] >= -42
 axisVdifLB[r,User,Clinic]: - x[r,User] + x[r,Clinic]
   - 49 zV[r,lb,User,Clinic] >= -42
 axisVdifLB[r,User,Location]: - x[r,User] + x[r,Location]
   - 49 zV[r,lb,User,Location] >= -42
 axisVdifLB[r,User,Therapist]: - x[r,User] + x[r,Therapist]
   - 49 zV[r,lb,User,Therapist] >= -42
 axisVdifLB[r,User,Instructor]: - x[r,User] + x[r,Instructor]
   - 49 zV[r,lb,User,Instructor] >= -42
 axisVdifLB[r,CourseSession,Clinic]: - x[r,CourseSession] + x[r,Clinic]
   - 51 zV[r,lb,CourseSession,Clinic] >= -42
 axisVdifLB[r,CourseSession,Location]: - x[r,CourseSession] + x[r,Location]
   - 51 zV[r,lb,CourseSession,Location] >= -42
 axisVdifLB[r,CourseSession,Therapist]: - x[r,CourseSession]
   + x[r,Therapist] - 51 zV[r,lb,CourseSession,Therapist] >= -42
 axisVdifLB[r,CourseSession,Instructor]: - x[r,CourseSession]
   + x[r,Instructor] - 51 zV[r,lb,CourseSession,Instructor] >= -42
 axisVdifLB[r,Clinic,Location]: - x[r,Clinic] + x[r,Location]
   - 50 zV[r,lb,Clinic,Location] >= -42
 axisVdifLB[r,Clinic,Therapist]: - x[r,Clinic] + x[r,Therapist]
   - 50 zV[r,lb,Clinic,Therapist] >= -42
 axisVdifLB[r,Clinic,Instructor]: - x[r,Clinic] + x[r,Instructor]
   - 50 zV[r,lb,Clinic,Instructor] >= -42
 axisVdifLB[r,Location,Therapist]: - x[r,Location] + x[r,Therapist]
   - 51 zV[r,lb,Location,Therapist] >= -42
 axisVdifLB[r,Location,Instructor]: - x[r,Location] + x[r,Instructor]
   - 51 zV[r,lb,Location,Instructor] >= -42
 axisVdifLB[r,Therapist,Instructor]: - x[r,Therapist] + x[r,Instructor]
   - 52 zV[r,lb,Therapist,Instructor] >= -42
 axisVdifLB[c,Client,CertificateTemplate]: - x[c,Client]
   + x[c,CertificateTemplate] - 53 zV[c,lb,Client,CertificateTemplate]
   >= -42
 axisVdifLB[c,Client,Course]: - x[c,Client] + x[c,Course]
   - 53 zV[c,lb,Client,Course] >= -42
 axisVdifLB[c,Client,Role]: - x[c,Client] + x[c,Role]
   - 53 zV[c,lb,Client,Role] >= -42
 axisVdifLB[c,Client,User]: - x[c,Client] + x[c,User]
   - 53 zV[c,lb,Client,User] >= -42
 axisVdifLB[c,Client,CourseSession]: - x[c,Client] + x[c,CourseSession]
   - 53 zV[c,lb,Client,CourseSession] >= -42
 axisVdifLB[c,Client,Clinic]: - x[c,Client] + x[c,Clinic]
   - 53 zV[c,lb,Client,Clinic] >= -42
 axisVdifLB[c,Client,Location]: - x[c,Client] + x[c,Location]
   - 53 zV[c,lb,Client,Location] >= -42
 axisVdifLB[c,Client,Therapist]: - x[c,Client] + x[c,Therapist]
   - 53 zV[c,lb,Client,Therapist] >= -42
 axisVdifLB[c,Client,Instructor]: - x[c,Client] + x[c,Instructor]
   - 53 zV[c,lb,Client,Instructor] >= -42
 axisVdifLB[c,CertificateTemplate,Course]: - x[c,CertificateTemplate]
   + x[c,Course] - 54 zV[c,lb,CertificateTemplate,Course] >= -42
 axisVdifLB[c,CertificateTemplate,Role]: - x[c,CertificateTemplate]
   + x[c,Role] - 54 zV[c,lb,CertificateTemplate,Role] >= -42
 axisVdifLB[c,CertificateTemplate,User]: - x[c,CertificateTemplate]
   + x[c,User] - 54 zV[c,lb,CertificateTemplate,User] >= -42
 axisVdifLB[c,CertificateTemplate,CourseSession]:
   - x[c,CertificateTemplate] + x[c,CourseSession]
   - 54 zV[c,lb,CertificateTemplate,CourseSession] >= -42
 axisVdifLB[c,CertificateTemplate,Clinic]: - x[c,CertificateTemplate]
   + x[c,Clinic] - 54 zV[c,lb,CertificateTemplate,Clinic] >= -42
 axisVdifLB[c,CertificateTemplate,Location]: - x[c,CertificateTemplate]
   + x[c,Location] - 54 zV[c,lb,CertificateTemplate,Location] >= -42
 axisVdifLB[c,CertificateTemplate,Therapist]: - x[c,CertificateTemplate]
   + x[c,Therapist] - 54 zV[c,lb,CertificateTemplate,Therapist] >= -42
 axisVdifLB[c,CertificateTemplate,Instructor]: - x[c,CertificateTemplate]
   + x[c,Instructor] - 54 zV[c,lb,CertificateTemplate,Instructor] >= -42
 axisVdifLB[c,Course,Role]: - x[c,Course] + x[c,Role]
   - 52 zV[c,lb,Course,Role] >= -42
 axisVdifLB[c,Course,User]: - x[c,Course] + x[c,User]
   - 52 zV[c,lb,Course,User] >= -42
 axisVdifLB[c,Course,CourseSession]: - x[c,Course] + x[c,CourseSession]
   - 52 zV[c,lb,Course,CourseSession] >= -42
 axisVdifLB[c,Course,Clinic]: - x[c,Course] + x[c,Clinic]
   - 52 zV[c,lb,Course,Clinic] >= -42
 axisVdifLB[c,Course,Location]: - x[c,Course] + x[c,Location]
   - 52 zV[c,lb,Course,Location] >= -42
 axisVdifLB[c,Course,Therapist]: - x[c,Course] + x[c,Therapist]
   - 52 zV[c,lb,Course,Therapist] >= -42
 axisVdifLB[c,Course,Instructor]: - x[c,Course] + x[c,Instructor]
   - 52 zV[c,lb,Course,Instructor] >= -42
 axisVdifLB[c,Role,User]: - x[c,Role] + x[c,User] - 52 zV[c,lb,Role,User]
   >= -42
 axisVdifLB[c,Role,CourseSession]: - x[c,Role] + x[c,CourseSession]
   - 52 zV[c,lb,Role,CourseSession] >= -42
 axisVdifLB[c,Role,Clinic]: - x[c,Role] + x[c,Clinic]
   - 52 zV[c,lb,Role,Clinic] >= -42
 axisVdifLB[c,Role,Location]: - x[c,Role] + x[c,Location]
   - 52 zV[c,lb,Role,Location] >= -42
 axisVdifLB[c,Role,Therapist]: - x[c,Role] + x[c,Therapist]
   - 52 zV[c,lb,Role,Therapist] >= -42
 axisVdifLB[c,Role,Instructor]: - x[c,Role] + x[c,Instructor]
   - 52 zV[c,lb,Role,Instructor] >= -42
 axisVdifLB[c,User,CourseSession]: - x[c,User] + x[c,CourseSession]
   - 53 zV[c,lb,User,CourseSession] >= -42
 axisVdifLB[c,User,Clinic]: - x[c,User] + x[c,Clinic]
   - 53 zV[c,lb,User,Clinic] >= -42
 axisVdifLB[c,User,Location]: - x[c,User] + x[c,Location]
   - 53 zV[c,lb,User,Location] >= -42
 axisVdifLB[c,User,Therapist]: - x[c,User] + x[c,Therapist]
   - 53 zV[c,lb,User,Therapist] >= -42
 axisVdifLB[c,User,Instructor]: - x[c,User] + x[c,Instructor]
   - 53 zV[c,lb,User,Instructor] >= -42
 axisVdifLB[c,CourseSession,Clinic]: - x[c,CourseSession] + x[c,Clinic]
   - 56 zV[c,lb,CourseSession,Clinic] >= -42
 axisVdifLB[c,CourseSession,Location]: - x[c,CourseSession] + x[c,Location]
   - 56 zV[c,lb,CourseSession,Location] >= -42
 axisVdifLB[c,CourseSession,Therapist]: - x[c,CourseSession]
   + x[c,Therapist] - 56 zV[c,lb,CourseSession,Therapist] >= -42
 axisVdifLB[c,CourseSession,Instructor]: - x[c,CourseSession]
   + x[c,Instructor] - 56 zV[c,lb,CourseSession,Instructor] >= -42
 axisVdifLB[c,Clinic,Location]: - x[c,Clinic] + x[c,Location]
   - 53 zV[c,lb,Clinic,Location] >= -42
 axisVdifLB[c,Clinic,Therapist]: - x[c,Clinic] + x[c,Therapist]
   - 53 zV[c,lb,Clinic,Therapist] >= -42
 axisVdifLB[c,Clinic,Instructor]: - x[c,Clinic] + x[c,Instructor]
   - 53 zV[c,lb,Clinic,Instructor] >= -42
 axisVdifLB[c,Location,Therapist]: - x[c,Location] + x[c,Therapist]
   - 53 zV[c,lb,Location,Therapist] >= -42
 axisVdifLB[c,Location,Instructor]: - x[c,Location] + x[c,Instructor]
   - 53 zV[c,lb,Location,Instructor] >= -42
 axisVdifLB[c,Therapist,Instructor]: - x[c,Therapist] + x[c,Instructor]
   - 53 zV[c,lb,Therapist,Instructor] >= -42
 axisVdifRT[r,Client,CertificateTemplate]: x[r,Client]
   - x[r,CertificateTemplate] - 49 zV[r,rt,Client,CertificateTemplate]
   >= -42
 axisVdifRT[r,Client,Course]: x[r,Client] - x[r,Course]
   - 48 zV[r,rt,Client,Course] >= -42
 axisVdifRT[r,Client,Role]: x[r,Client] - x[r,Role]
   - 48 zV[r,rt,Client,Role] >= -42
 axisVdifRT[r,Client,User]: x[r,Client] - x[r,User]
   - 49 zV[r,rt,Client,User] >= -42
 axisVdifRT[r,Client,CourseSession]: x[r,Client] - x[r,CourseSession]
   - 51 zV[r,rt,Client,CourseSession] >= -42
 axisVdifRT[r,Client,Clinic]: x[r,Client] - x[r,Clinic]
   - 50 zV[r,rt,Client,Clinic] >= -42
 axisVdifRT[r,Client,Location]: x[r,Client] - x[r,Location]
   - 51 zV[r,rt,Client,Location] >= -42
 axisVdifRT[r,Client,Therapist]: x[r,Client] - x[r,Therapist]
   - 52 zV[r,rt,Client,Therapist] >= -42
 axisVdifRT[r,Client,Instructor]: x[r,Client] - x[r,Instructor]
   - 52 zV[r,rt,Client,Instructor] >= -42
 axisVdifRT[r,CertificateTemplate,Course]: x[r,CertificateTemplate]
   - x[r,Course] - 48 zV[r,rt,CertificateTemplate,Course] >= -42
 axisVdifRT[r,CertificateTemplate,Role]: x[r,CertificateTemplate]
   - x[r,Role] - 48 zV[r,rt,CertificateTemplate,Role] >= -42
 axisVdifRT[r,CertificateTemplate,User]: x[r,CertificateTemplate]
   - x[r,User] - 49 zV[r,rt,CertificateTemplate,User] >= -42
 axisVdifRT[r,CertificateTemplate,CourseSession]: x[r,CertificateTemplate]
   - x[r,CourseSession] - 51 zV[r,rt,CertificateTemplate,CourseSession]
   >= -42
 axisVdifRT[r,CertificateTemplate,Clinic]: x[r,CertificateTemplate]
   - x[r,Clinic] - 50 zV[r,rt,CertificateTemplate,Clinic] >= -42
 axisVdifRT[r,CertificateTemplate,Location]: x[r,CertificateTemplate]
   - x[r,Location] - 51 zV[r,rt,CertificateTemplate,Location] >= -42
 axisVdifRT[r,CertificateTemplate,Therapist]: x[r,CertificateTemplate]
   - x[r,Therapist] - 52 zV[r,rt,CertificateTemplate,Therapist] >= -42
 axisVdifRT[r,CertificateTemplate,Instructor]: x[r,CertificateTemplate]
   - x[r,Instructor] - 52 zV[r,rt,CertificateTemplate,Instructor] >= -42
 axisVdifRT[r,Course,Role]: x[r,Course] - x[r,Role]
   - 48 zV[r,rt,Course,Role] >= -42
 axisVdifRT[r,Course,User]: x[r,Course] - x[r,User]
   - 49 zV[r,rt,Course,User] >= -42
 axisVdifRT[r,Course,CourseSession]: x[r,Course] - x[r,CourseSession]
   - 51 zV[r,rt,Course,CourseSession] >= -42
 axisVdifRT[r,Course,Clinic]: x[r,Course] - x[r,Clinic]
   - 50 zV[r,rt,Course,Clinic] >= -42
 axisVdifRT[r,Course,Location]: x[r,Course] - x[r,Location]
   - 51 zV[r,rt,Course,Location] >= -42
 axisVdifRT[r,Course,Therapist]: x[r,Course] - x[r,Therapist]
   - 52 zV[r,rt,Course,Therapist] >= -42
 axisVdifRT[r,Course,Instructor]: x[r,Course] - x[r,Instructor]
   - 52 zV[r,rt,Course,Instructor] >= -42
 axisVdifRT[r,Role,User]: x[r,Role] - x[r,User] - 49 zV[r,rt,Role,User]
   >= -42
 axisVdifRT[r,Role,CourseSession]: x[r,Role] - x[r,CourseSession]
   - 51 zV[r,rt,Role,CourseSession] >= -42
 axisVdifRT[r,Role,Clinic]: x[r,Role] - x[r,Clinic]
   - 50 zV[r,rt,Role,Clinic] >= -42
 axisVdifRT[r,Role,Location]: x[r,Role] - x[r,Location]
   - 51 zV[r,rt,Role,Location] >= -42
 axisVdifRT[r,Role,Therapist]: x[r,Role] - x[r,Therapist]
   - 52 zV[r,rt,Role,Therapist] >= -42
 axisVdifRT[r,Role,Instructor]: x[r,Role] - x[r,Instructor]
   - 52 zV[r,rt,Role,Instructor] >= -42
 axisVdifRT[r,User,CourseSession]: x[r,User] - x[r,CourseSession]
   - 51 zV[r,rt,User,CourseSession] >= -42
 axisVdifRT[r,User,Clinic]: x[r,User] - x[r,Clinic]
   - 50 zV[r,rt,User,Clinic] >= -42
 axisVdifRT[r,User,Location]: x[r,User] - x[r,Location]
   - 51 zV[r,rt,User,Location] >= -42
 axisVdifRT[r,User,Therapist]: x[r,User] - x[r,Therapist]
   - 52 zV[r,rt,User,Therapist] >= -42
 axisVdifRT[r,User,Instructor]: x[r,User] - x[r,Instructor]
   - 52 zV[r,rt,User,Instructor] >= -42
 axisVdifRT[r,CourseSession,Clinic]: x[r,CourseSession] - x[r,Clinic]
   - 50 zV[r,rt,CourseSession,Clinic] >= -42
 axisVdifRT[r,CourseSession,Location]: x[r,CourseSession] - x[r,Location]
   - 51 zV[r,rt,CourseSession,Location] >= -42
 axisVdifRT[r,CourseSession,Therapist]: x[r,CourseSession] - x[r,Therapist]
   - 52 zV[r,rt,CourseSession,Therapist] >= -42
 axisVdifRT[r,CourseSession,Instructor]: x[r,CourseSession]
   - x[r,Instructor] - 52 zV[r,rt,CourseSession,Instructor] >= -42
 axisVdifRT[r,Clinic,Location]: x[r,Clinic] - x[r,Location]
   - 51 zV[r,rt,Clinic,Location] >= -42
 axisVdifRT[r,Clinic,Therapist]: x[r,Clinic] - x[r,Therapist]
   - 52 zV[r,rt,Clinic,Therapist] >= -42
 axisVdifRT[r,Clinic,Instructor]: x[r,Clinic] - x[r,Instructor]
   - 52 zV[r,rt,Clinic,Instructor] >= -42
 axisVdifRT[r,Location,Therapist]: x[r,Location] - x[r,Therapist]
   - 52 zV[r,rt,Location,Therapist] >= -42
 axisVdifRT[r,Location,Instructor]: x[r,Location] - x[r,Instructor]
   - 52 zV[r,rt,Location,Instructor] >= -42
 axisVdifRT[r,Therapist,Instructor]: x[r,Therapist] - x[r,Instructor]
   - 52 zV[r,rt,Therapist,Instructor] >= -42
 axisVdifRT[c,Client,CertificateTemplate]: x[c,Client]
   - x[c,CertificateTemplate] - 54 zV[c,rt,Client,CertificateTemplate]
   >= -42
 axisVdifRT[c,Client,Course]: x[c,Client] - x[c,Course]
   - 52 zV[c,rt,Client,Course] >= -42
 axisVdifRT[c,Client,Role]: x[c,Client] - x[c,Role]
   - 52 zV[c,rt,Client,Role] >= -42
 axisVdifRT[c,Client,User]: x[c,Client] - x[c,User]
   - 53 zV[c,rt,Client,User] >= -42
 axisVdifRT[c,Client,CourseSession]: x[c,Client] - x[c,CourseSession]
   - 56 zV[c,rt,Client,CourseSession] >= -42
 axisVdifRT[c,Client,Clinic]: x[c,Client] - x[c,Clinic]
   - 53 zV[c,rt,Client,Clinic] >= -42
 axisVdifRT[c,Client,Location]: x[c,Client] - x[c,Location]
   - 53 zV[c,rt,Client,Location] >= -42
 axisVdifRT[c,Client,Therapist]: x[c,Client] - x[c,Therapist]
   - 53 zV[c,rt,Client,Therapist] >= -42
 axisVdifRT[c,Client,Instructor]: x[c,Client] - x[c,Instructor]
   - 54 zV[c,rt,Client,Instructor] >= -42
 axisVdifRT[c,CertificateTemplate,Course]: x[c,CertificateTemplate]
   - x[c,Course] - 52 zV[c,rt,CertificateTemplate,Course] >= -42
 axisVdifRT[c,CertificateTemplate,Role]: x[c,CertificateTemplate]
   - x[c,Role] - 52 zV[c,rt,CertificateTemplate,Role] >= -42
 axisVdifRT[c,CertificateTemplate,User]: x[c,CertificateTemplate]
   - x[c,User] - 53 zV[c,rt,CertificateTemplate,User] >= -42
 axisVdifRT[c,CertificateTemplate,CourseSession]: x[c,CertificateTemplate]
   - x[c,CourseSession] - 56 zV[c,rt,CertificateTemplate,CourseSession]
   >= -42
 axisVdifRT[c,CertificateTemplate,Clinic]: x[c,CertificateTemplate]
   - x[c,Clinic] - 53 zV[c,rt,CertificateTemplate,Clinic] >= -42
 axisVdifRT[c,CertificateTemplate,Location]: x[c,CertificateTemplate]
   - x[c,Location] - 53 zV[c,rt,CertificateTemplate,Location] >= -42
 axisVdifRT[c,CertificateTemplate,Therapist]: x[c,CertificateTemplate]
   - x[c,Therapist] - 53 zV[c,rt,CertificateTemplate,Therapist] >= -42
 axisVdifRT[c,CertificateTemplate,Instructor]: x[c,CertificateTemplate]
   - x[c,Instructor] - 54 zV[c,rt,CertificateTemplate,Instructor] >= -42
 axisVdifRT[c,Course,Role]: x[c,Course] - x[c,Role]
   - 52 zV[c,rt,Course,Role] >= -42
 axisVdifRT[c,Course,User]: x[c,Course] - x[c,User]
   - 53 zV[c,rt,Course,User] >= -42
 axisVdifRT[c,Course,CourseSession]: x[c,Course] - x[c,CourseSession]
   - 56 zV[c,rt,Course,CourseSession] >= -42
 axisVdifRT[c,Course,Clinic]: x[c,Course] - x[c,Clinic]
   - 53 zV[c,rt,Course,Clinic] >= -42
 axisVdifRT[c,Course,Location]: x[c,Course] - x[c,Location]
   - 53 zV[c,rt,Course,Location] >= -42
 axisVdifRT[c,Course,Therapist]: x[c,Course] - x[c,Therapist]
   - 53 zV[c,rt,Course,Therapist] >= -42
 axisVdifRT[c,Course,Instructor]: x[c,Course] - x[c,Instructor]
   - 54 zV[c,rt,Course,Instructor] >= -42
 axisVdifRT[c,Role,User]: x[c,Role] - x[c,User] - 53 zV[c,rt,Role,User]
   >= -42
 axisVdifRT[c,Role,CourseSession]: x[c,Role] - x[c,CourseSession]
   - 56 zV[c,rt,Role,CourseSession] >= -42
 axisVdifRT[c,Role,Clinic]: x[c,Role] - x[c,Clinic]
   - 53 zV[c,rt,Role,Clinic] >= -42
 axisVdifRT[c,Role,Location]: x[c,Role] - x[c,Location]
   - 53 zV[c,rt,Role,Location] >= -42
 axisVdifRT[c,Role,Therapist]: x[c,Role] - x[c,Therapist]
   - 53 zV[c,rt,Role,Therapist] >= -42
 axisVdifRT[c,Role,Instructor]: x[c,Role] - x[c,Instructor]
   - 54 zV[c,rt,Role,Instructor] >= -42
 axisVdifRT[c,User,CourseSession]: x[c,User] - x[c,CourseSession]
   - 56 zV[c,rt,User,CourseSession] >= -42
 axisVdifRT[c,User,Clinic]: x[c,User] - x[c,Clinic]
   - 53 zV[c,rt,User,Clinic] >= -42
 axisVdifRT[c,User,Location]: x[c,User] - x[c,Location]
   - 53 zV[c,rt,User,Location] >= -42
 axisVdifRT[c,User,Therapist]: x[c,User] - x[c,Therapist]
   - 53 zV[c,rt,User,Therapist] >= -42
 axisVdifRT[c,User,Instructor]: x[c,User] - x[c,Instructor]
   - 54 zV[c,rt,User,Instructor] >= -42
 axisVdifRT[c,CourseSession,Clinic]: x[c,CourseSession] - x[c,Clinic]
   - 53 zV[c,rt,CourseSession,Clinic] >= -42
 axisVdifRT[c,CourseSession,Location]: x[c,CourseSession] - x[c,Location]
   - 53 zV[c,rt,CourseSession,Location] >= -42
 axisVdifRT[c,CourseSession,Therapist]: x[c,CourseSession] - x[c,Therapist]
   - 53 zV[c,rt,CourseSession,Therapist] >= -42
 axisVdifRT[c,CourseSession,Instructor]: x[c,CourseSession]
   - x[c,Instructor] - 54 zV[c,rt,CourseSession,Instructor] >= -42
 axisVdifRT[c,Clinic,Location]: x[c,Clinic] - x[c,Location]
   - 53 zV[c,rt,Clinic,Location] >= -42
 axisVdifRT[c,Clinic,Therapist]: x[c,Clinic] - x[c,Therapist]
   - 53 zV[c,rt,Clinic,Therapist] >= -42
 axisVdifRT[c,Clinic,Instructor]: x[c,Clinic] - x[c,Instructor]
   - 54 zV[c,rt,Clinic,Instructor] >= -42
 axisVdifRT[c,Location,Therapist]: x[c,Location] - x[c,Therapist]
   - 53 zV[c,rt,Location,Therapist] >= -42
 axisVdifRT[c,Location,Instructor]: x[c,Location] - x[c,Instructor]
   - 54 zV[c,rt,Location,Instructor] >= -42
 axisVdifRT[c,Therapist,Instructor]: x[c,Therapist] - x[c,Instructor]
   - 54 zV[c,rt,Therapist,Instructor] >= -42
 compDistAxis1[r,Client,CertificateTemplate]: - x[r,Client]
   + x[r,CertificateTemplate] + dist[r,Client,CertificateTemplate] >= 1
 compDistAxis1[r,Client,CourseSession]: - x[r,Client] + x[r,CourseSession]
   + dist[r,Client,CourseSession] >= 0
 compDistAxis1[r,Client,Instructor]: - x[r,Client] + x[r,Instructor]
   + dist[r,Client,Instructor] >= -0.5
 compDistAxis1[r,Client,Location]: - x[r,Client] + x[r,Location]
   + dist[r,Client,Location] >= 0
 compDistAxis1[r,Client,User]: - x[r,Client] + x[r,User]
   + dist[r,Client,User] >= 1
 compDistAxis1[r,CertificateTemplate,CourseSession]:
   - x[r,CertificateTemplate] + x[r,CourseSession]
   + dist[r,CertificateTemplate,CourseSession] >= -1
 compDistAxis1[r,Course,CourseSession]: - x[r,Course] + x[r,CourseSession]
   + dist[r,Course,CourseSession] >= -1.5
 compDistAxis1[r,CourseSession,Instructor]: - x[r,CourseSession]
   + x[r,Instructor] + dist[r,CourseSession,Instructor] >= -0.5
 compDistAxis1[r,Instructor,Location]: x[r,Location] - x[r,Instructor]
   + dist[r,Instructor,Location] >= 0.5
 compDistAxis1[r,Clinic,Location]: - x[r,Clinic] + x[r,Location]
   + dist[r,Clinic,Location] >= -0.5
 compDistAxis1[r,Clinic,Therapist]: - x[r,Clinic] + x[r,Therapist]
   + dist[r,Clinic,Therapist] >= -1
 compDistAxis1[r,Location,Therapist]: - x[r,Location] + x[r,Therapist]
   + dist[r,Location,Therapist] >= -0.5
 compDistAxis1[r,Location,CourseSession]: x[r,CourseSession]
   - x[r,Location] + dist[r,Location,CourseSession] >= 0
 compDistAxis1[r,Therapist,CourseSession]: x[r,CourseSession]
   - x[r,Therapist] + dist[r,Therapist,CourseSession] >= 0.5
 compDistAxis1[r,Role,User]: - x[r,Role] + x[r,User] + dist[r,Role,User]
   >= -0.5
 compDistAxis1[r,User,Instructor]: - x[r,User] + x[r,Instructor]
   + dist[r,User,Instructor] >= -1.5
 compDistAxis1[r,User,Therapist]: - x[r,User] + x[r,Therapist]
   + dist[r,User,Therapist] >= -1.5
 compDistAxis1[c,Client,CertificateTemplate]: - x[c,Client]
   + x[c,CertificateTemplate] + dist[c,Client,CertificateTemplate] >= -0.5
 compDistAxis1[c,Client,CourseSession]: - x[c,Client] + x[c,CourseSession]
   + dist[c,Client,CourseSession] >= -1.5
 compDistAxis1[c,Client,Instructor]: - x[c,Client] + x[c,Instructor]
   + dist[c,Client,Instructor] >= -0.5
 compDistAxis1[c,Client,Location]: - x[c,Client] + x[c,Location]
   + dist[c,Client,Location] >= 0
 compDistAxis1[c,Client,User]: - x[c,Client] + x[c,User]
   + dist[c,Client,User] >= 0
 compDistAxis1[c,CertificateTemplate,CourseSession]:
   - x[c,CertificateTemplate] + x[c,CourseSession]
   + dist[c,CertificateTemplate,CourseSession] >= -1
 compDistAxis1[c,Course,CourseSession]: - x[c,Course] + x[c,CourseSession]
   + dist[c,Course,CourseSession] >= -2
 compDistAxis1[c,CourseSession,Instructor]: - x[c,CourseSession]
   + x[c,Instructor] + dist[c,CourseSession,Instructor] >= 1
 compDistAxis1[c,Instructor,Location]: x[c,Location] - x[c,Instructor]
   + dist[c,Instructor,Location] >= 0.5
 compDistAxis1[c,Clinic,Location]: - x[c,Clinic] + x[c,Location]
   + dist[c,Clinic,Location] >= 0
 compDistAxis1[c,Clinic,Therapist]: - x[c,Clinic] + x[c,Therapist]
   + dist[c,Clinic,Therapist] >= 0
 compDistAxis1[c,Location,Therapist]: - x[c,Location] + x[c,Therapist]
   + dist[c,Location,Therapist] >= 0
 compDistAxis1[c,Location,CourseSession]: x[c,CourseSession]
   - x[c,Location] + dist[c,Location,CourseSession] >= -1.5
 compDistAxis1[c,Therapist,CourseSession]: x[c,CourseSession]
   - x[c,Therapist] + dist[c,Therapist,CourseSession] >= -1.5
 compDistAxis1[c,Role,User]: - x[c,Role] + x[c,User] + dist[c,Role,User]
   >= -0.5
 compDistAxis1[c,User,Instructor]: - x[c,User] + x[c,Instructor]
   + dist[c,User,Instructor] >= -0.5
 compDistAxis1[c,User,Therapist]: - x[c,User] + x[c,Therapist]
   + dist[c,User,Therapist] >= 0
 compDistAxis2[r,Client,CertificateTemplate]: x[r,Client]
   - x[r,CertificateTemplate] + dist[r,Client,CertificateTemplate] >= -1
 compDistAxis2[r,Client,CourseSession]: x[r,Client] - x[r,CourseSession]
   + dist[r,Client,CourseSession] >= 0
 compDistAxis2[r,Client,Instructor]: x[r,Client] - x[r,Instructor]
   + dist[r,Client,Instructor] >= 0.5
 compDistAxis2[r,Client,Location]: x[r,Client] - x[r,Location]
   + dist[r,Client,Location] >= 0
 compDistAxis2[r,Client,User]: x[r,Client] - x[r,User]
   + dist[r,Client,User] >= -1
 compDistAxis2[r,CertificateTemplate,CourseSession]:
   x[r,CertificateTemplate] - x[r,CourseSession]
   + dist[r,CertificateTemplate,CourseSession] >= 1
 compDistAxis2[r,Course,CourseSession]: x[r,Course] - x[r,CourseSession]
   + dist[r,Course,CourseSession] >= 1.5
 compDistAxis2[r,CourseSession,Instructor]: x[r,CourseSession]
   - x[r,Instructor] + dist[r,CourseSession,Instructor] >= 0.5
 compDistAxis2[r,Instructor,Location]: - x[r,Location] + x[r,Instructor]
   + dist[r,Instructor,Location] >= -0.5
 compDistAxis2[r,Clinic,Location]: x[r,Clinic] - x[r,Location]
   + dist[r,Clinic,Location] >= 0.5
 compDistAxis2[r,Clinic,Therapist]: x[r,Clinic] - x[r,Therapist]
   + dist[r,Clinic,Therapist] >= 1
 compDistAxis2[r,Location,Therapist]: x[r,Location] - x[r,Therapist]
   + dist[r,Location,Therapist] >= 0.5
 compDistAxis2[r,Location,CourseSession]: - x[r,CourseSession]
   + x[r,Location] + dist[r,Location,CourseSession] >= 0
 compDistAxis2[r,Therapist,CourseSession]: - x[r,CourseSession]
   + x[r,Therapist] + dist[r,Therapist,CourseSession] >= -0.5
 compDistAxis2[r,Role,User]: x[r,Role] - x[r,User] + dist[r,Role,User]
   >= 0.5
 compDistAxis2[r,User,Instructor]: x[r,User] - x[r,Instructor]
   + dist[r,User,Instructor] >= 1.5
 compDistAxis2[r,User,Therapist]: x[r,User] - x[r,Therapist]
   + dist[r,User,Therapist] >= 1.5
 compDistAxis2[c,Client,CertificateTemplate]: x[c,Client]
   - x[c,CertificateTemplate] + dist[c,Client,CertificateTemplate] >= 0.5
 compDistAxis2[c,Client,CourseSession]: x[c,Client] - x[c,CourseSession]
   + dist[c,Client,CourseSession] >= 1.5
 compDistAxis2[c,Client,Instructor]: x[c,Client] - x[c,Instructor]
   + dist[c,Client,Instructor] >= 0.5
 compDistAxis2[c,Client,Location]: x[c,Client] - x[c,Location]
   + dist[c,Client,Location] >= 0
 compDistAxis2[c,Client,User]: x[c,Client] - x[c,User]
   + dist[c,Client,User] >= 0
 compDistAxis2[c,CertificateTemplate,CourseSession]:
   x[c,CertificateTemplate] - x[c,CourseSession]
   + dist[c,CertificateTemplate,CourseSession] >= 1
 compDistAxis2[c,Course,CourseSession]: x[c,Course] - x[c,CourseSession]
   + dist[c,Course,CourseSession] >= 2
 compDistAxis2[c,CourseSession,Instructor]: x[c,CourseSession]
   - x[c,Instructor] + dist[c,CourseSession,Instructor] >= -1
 compDistAxis2[c,Instructor,Location]: - x[c,Location] + x[c,Instructor]
   + dist[c,Instructor,Location] >= -0.5
 compDistAxis2[c,Clinic,Location]: x[c,Clinic] - x[c,Location]
   + dist[c,Clinic,Location] >= 0
 compDistAxis2[c,Clinic,Therapist]: x[c,Clinic] - x[c,Therapist]
   + dist[c,Clinic,Therapist] >= 0
 compDistAxis2[c,Location,Therapist]: x[c,Location] - x[c,Therapist]
   + dist[c,Location,Therapist] >= 0
 compDistAxis2[c,Location,CourseSession]: - x[c,CourseSession]
   + x[c,Location] + dist[c,Location,CourseSession] >= 1.5
 compDistAxis2[c,Therapist,CourseSession]: - x[c,CourseSession]
   + x[c,Therapist] + dist[c,Therapist,CourseSession] >= 1.5
 compDistAxis2[c,Role,User]: x[c,Role] - x[c,User] + dist[c,Role,User]
   >= 0.5
 compDistAxis2[c,User,Instructor]: x[c,User] - x[c,Instructor]
   + dist[c,User,Instructor] >= 0.5
 compDistAxis2[c,User,Therapist]: x[c,User] - x[c,Therapist]
   + dist[c,User,Therapist] >= 0
 DistSumLB[Client,CertificateTemplate]: dist[r,Client,CertificateTemplate]
   + dist[c,Client,CertificateTemplate] >= 8
 DistSumLB[Client,CourseSession]: dist[r,Client,CourseSession]
   + dist[c,Client,CourseSession] >= 9
 DistSumLB[Client,Instructor]: dist[r,Client,Instructor]
   + dist[c,Client,Instructor] >= 9.5
 DistSumLB[Client,Location]: dist[r,Client,Location]
   + dist[c,Client,Location] >= 9
 DistSumLB[Client,User]: dist[r,Client,User] + dist[c,Client,User] >= 8
 DistSumLB[CertificateTemplate,CourseSession]:
   dist[r,CertificateTemplate,CourseSession]
   + dist[c,CertificateTemplate,CourseSession] >= 8
 DistSumLB[Course,CourseSession]: dist[r,Course,CourseSession]
   + dist[c,Course,CourseSession] >= 7.5
 DistSumLB[CourseSession,Instructor]: dist[r,CourseSession,Instructor]
   + dist[c,CourseSession,Instructor] >= 9.5
 DistSumLB[Instructor,Location]: dist[r,Instructor,Location]
   + dist[c,Instructor,Location] >= 9.5
 DistSumLB[Clinic,Location]: dist[r,Clinic,Location]
   + dist[c,Clinic,Location] >= 8.5
 DistSumLB[Clinic,Therapist]: dist[r,Clinic,Therapist]
   + dist[c,Clinic,Therapist] >= 9
 DistSumLB[Location,Therapist]: dist[r,Location,Therapist]
   + dist[c,Location,Therapist] >= 9.5
 DistSumLB[Location,CourseSession]: dist[r,Location,CourseSession]
   + dist[c,Location,CourseSession] >= 9
 DistSumLB[Therapist,CourseSession]: dist[r,Therapist,CourseSession]
   + dist[c,Therapist,CourseSession] >= 9.5
 DistSumLB[Role,User]: dist[r,Role,User] + dist[c,Role,User] >= 6.5
 DistSumLB[User,Instructor]: dist[r,User,Instructor]
   + dist[c,User,Instructor] >= 8.5
 DistSumLB[User,Therapist]: dist[r,User,Therapist] + dist[c,User,Therapist]
   >= 8.5
 CenterDistSumLB: center_dist[r,Client]
   + center_dist[r,CertificateTemplate] + center_dist[r,Course]
   + center_dist[r,Role] + center_dist[r,User]
   + center_dist[r,CourseSession] + center_dist[r,Clinic]
   + center_dist[r,Location] + center_dist[r,Therapist]
   + center_dist[r,Instructor] + center_dist[c,Client]
   + center_dist[c,CertificateTemplate] + center_dist[c,Course]
   + center_dist[c,Role] + center_dist[c,User]
   + center_dist[c,CourseSession] + center_dist[c,Clinic]
   + center_dist[c,Location] + center_dist[c,Therapist]
   + center_dist[c,Instructor] >= 30
 DistAxisLB1[r,Client,CertificateTemplate]:
   dist[r,Client,CertificateTemplate]
   + 8 zV[c,rt,Client,CertificateTemplate]
   + 8 zV[c,lb,Client,CertificateTemplate] >= 8
 DistAxisLB1[r,Client,CourseSession]: dist[r,Client,CourseSession]
   + 9 zV[c,rt,Client,CourseSession] + 9 zV[c,lb,Client,CourseSession]
   >= 9
 DistAxisLB1[r,Client,Instructor]: dist[r,Client,Instructor]
   + 9.5 zV[c,rt,Client,Instructor] + 9.5 zV[c,lb,Client,Instructor]
   >= 9.5
 DistAxisLB1[r,Client,Location]: dist[r,Client,Location]
   + 9 zV[c,rt,Client,Location] + 9 zV[c,lb,Client,Location] >= 9
 DistAxisLB1[r,Client,User]: dist[r,Client,User] + 8 zV[c,rt,Client,User]
   + 8 zV[c,lb,Client,User] >= 8
 DistAxisLB1[r,CertificateTemplate,CourseSession]:
   dist[r,CertificateTemplate,CourseSession]
   + 8 zV[c,rt,CertificateTemplate,CourseSession]
   + 8 zV[c,lb,CertificateTemplate,CourseSession] >= 8
 DistAxisLB1[r,Course,CourseSession]: dist[r,Course,CourseSession]
   + 7.5 zV[c,rt,Course,CourseSession] + 7.5 zV[c,lb,Course,CourseSession]
   >= 7.5
 DistAxisLB1[r,CourseSession,Instructor]: dist[r,CourseSession,Instructor]
   + 9.5 zV[c,rt,CourseSession,Instructor]
   + 9.5 zV[c,lb,CourseSession,Instructor] >= 9.5
 DistAxisLB1[r,Instructor,Location]: dist[r,Instructor,Location]
   + 9.5 zV[c,rt,Location,Instructor] + 9.5 zV[c,lb,Location,Instructor]
   >= 9.5
 DistAxisLB1[r,Clinic,Location]: dist[r,Clinic,Location]
   + 8.5 zV[c,rt,Clinic,Location] + 8.5 zV[c,lb,Clinic,Location] >= 8.5
 DistAxisLB1[r,Clinic,Therapist]: dist[r,Clinic,Therapist]
   + 9 zV[c,rt,Clinic,Therapist] + 9 zV[c,lb,Clinic,Therapist] >= 9
 DistAxisLB1[r,Location,Therapist]: dist[r,Location,Therapist]
   + 9.5 zV[c,rt,Location,Therapist] + 9.5 zV[c,lb,Location,Therapist]
   >= 9.5
 DistAxisLB1[r,Location,CourseSession]: dist[r,Location,CourseSession]
   + 9 zV[c,rt,CourseSession,Location] + 9 zV[c,lb,CourseSession,Location]
   >= 9
 DistAxisLB1[r,Therapist,CourseSession]: dist[r,Therapist,CourseSession]
   + 9.5 zV[c,rt,CourseSession,Therapist]
   + 9.5 zV[c,lb,CourseSession,Therapist] >= 9.5
 DistAxisLB1[r,Role,User]: dist[r,Role,User] + 6.5 zV[c,rt,Role,User]
   + 6.5 zV[c,lb,Role,User] >= 6.5
 DistAxisLB1[r,User,Instructor]: dist[r,User,Instructor]
   + 8.5 zV[c,rt,User,Instructor] + 8.5 zV[c,lb,User,Instructor] >= 8.5
 DistAxisLB1[r,User,Therapist]: dist[r,User,Therapist]
   + 8.5 zV[c,rt,User,Therapist] + 8.5 zV[c,lb,User,Therapist] >= 8.5
 DistAxisLB1[c,Client,CertificateTemplate]:
   dist[c,Client,CertificateTemplate]
   + 11.5 zV[r,rt,Client,CertificateTemplate]
   + 11.5 zV[r,lb,Client,CertificateTemplate] >= 11.5
 DistAxisLB1[c,Client,CourseSession]: dist[c,Client,CourseSession]
   + 12.5 zV[r,rt,Client,CourseSession]
   + 12.5 zV[r,lb,Client,CourseSession] >= 12.5
 DistAxisLB1[c,Client,Instructor]: dist[c,Client,Instructor]
   + 11.5 zV[r,rt,Client,Instructor] + 11.5 zV[r,lb,Client,Instructor]
   >= 11.5
 DistAxisLB1[c,Client,Location]: dist[c,Client,Location]
   + 11 zV[r,rt,Client,Location] + 11 zV[r,lb,Client,Location] >= 11
 DistAxisLB1[c,Client,User]: dist[c,Client,User] + 11 zV[r,rt,Client,User]
   + 11 zV[r,lb,Client,User] >= 11
 DistAxisLB1[c,CertificateTemplate,CourseSession]:
   dist[c,CertificateTemplate,CourseSession]
   + 13 zV[r,rt,CertificateTemplate,CourseSession]
   + 13 zV[r,lb,CertificateTemplate,CourseSession] >= 13
 DistAxisLB1[c,Course,CourseSession]: dist[c,Course,CourseSession]
   + 12 zV[r,rt,Course,CourseSession] + 12 zV[r,lb,Course,CourseSession]
   >= 12
 DistAxisLB1[c,CourseSession,Instructor]: dist[c,CourseSession,Instructor]
   + 13 zV[r,rt,CourseSession,Instructor]
   + 13 zV[r,lb,CourseSession,Instructor] >= 13
 DistAxisLB1[c,Instructor,Location]: dist[c,Instructor,Location]
   + 11.5 zV[r,rt,Location,Instructor] + 11.5 zV[r,lb,Location,Instructor]
   >= 11.5
 DistAxisLB1[c,Clinic,Location]: dist[c,Clinic,Location]
   + 11 zV[r,rt,Clinic,Location] + 11 zV[r,lb,Clinic,Location] >= 11
 DistAxisLB1[c,Clinic,Therapist]: dist[c,Clinic,Therapist]
   + 11 zV[r,rt,Clinic,Therapist] + 11 zV[r,lb,Clinic,Therapist] >= 11
 DistAxisLB1[c,Location,Therapist]: dist[c,Location,Therapist]
   + 11 zV[r,rt,Location,Therapist] + 11 zV[r,lb,Location,Therapist] >= 11
 DistAxisLB1[c,Location,CourseSession]: dist[c,Location,CourseSession]
   + 12.5 zV[r,rt,CourseSession,Location]
   + 12.5 zV[r,lb,CourseSession,Location] >= 12.5
 DistAxisLB1[c,Therapist,CourseSession]: dist[c,Therapist,CourseSession]
   + 12.5 zV[r,rt,CourseSession,Therapist]
   + 12.5 zV[r,lb,CourseSession,Therapist] >= 12.5
 DistAxisLB1[c,Role,User]: dist[c,Role,User] + 10.5 zV[r,rt,Role,User]
   + 10.5 zV[r,lb,Role,User] >= 10.5
 DistAxisLB1[c,User,Instructor]: dist[c,User,Instructor]
   + 11.5 zV[r,rt,User,Instructor] + 11.5 zV[r,lb,User,Instructor] >= 11.5
 DistAxisLB1[c,User,Therapist]: dist[c,User,Therapist]
   + 11 zV[r,rt,User,Therapist] + 11 zV[r,lb,User,Therapist] >= 11
 triangle1[Client,CertificateTemplate,Course,r]:
   zV[r,rt,Client,CertificateTemplate] + zV[r,lb,Client,Course]
   + zV[r,rt,CertificateTemplate,Course] <= 2
 triangle1[Client,CertificateTemplate,Course,c]:
   zV[c,rt,Client,CertificateTemplate] + zV[c,lb,Client,Course]
   + zV[c,rt,CertificateTemplate,Course] <= 2
 triangle1[Client,CertificateTemplate,Role,r]:
   zV[r,rt,Client,CertificateTemplate] + zV[r,lb,Client,Role]
   + zV[r,rt,CertificateTemplate,Role] <= 2
 triangle1[Client,CertificateTemplate,Role,c]:
   zV[c,rt,Client,CertificateTemplate] + zV[c,lb,Client,Role]
   + zV[c,rt,CertificateTemplate,Role] <= 2
 triangle1[Client,CertificateTemplate,User,r]:
   zV[r,rt,Client,CertificateTemplate] + zV[r,lb,Client,User]
   + zV[r,rt,CertificateTemplate,User] <= 2
 triangle1[Client,CertificateTemplate,User,c]:
   zV[c,rt,Client,CertificateTemplate] + zV[c,lb,Client,User]
   + zV[c,rt,CertificateTemplate,User] <= 2
 triangle1[Client,CertificateTemplate,CourseSession,r]:
   zV[r,rt,Client,CertificateTemplate] + zV[r,lb,Client,CourseSession]
   + zV[r,rt,CertificateTemplate,CourseSession] <= 2
 triangle1[Client,CertificateTemplate,CourseSession,c]:
   zV[c,rt,Client,CertificateTemplate] + zV[c,lb,Client,CourseSession]
   + zV[c,rt,CertificateTemplate,CourseSession] <= 2
 triangle1[Client,CertificateTemplate,Clinic,r]:
   zV[r,rt,Client,CertificateTemplate] + zV[r,lb,Client,Clinic]
   + zV[r,rt,CertificateTemplate,Clinic] <= 2
 triangle1[Client,CertificateTemplate,Clinic,c]:
   zV[c,rt,Client,CertificateTemplate] + zV[c,lb,Client,Clinic]
   + zV[c,rt,CertificateTemplate,Clinic] <= 2
 triangle1[Client,CertificateTemplate,Location,r]:
   zV[r,rt,Client,CertificateTemplate] + zV[r,lb,Client,Location]
   + zV[r,rt,CertificateTemplate,Location] <= 2
 triangle1[Client,CertificateTemplate,Location,c]:
   zV[c,rt,Client,CertificateTemplate] + zV[c,lb,Client,Location]
   + zV[c,rt,CertificateTemplate,Location] <= 2
 triangle1[Client,CertificateTemplate,Therapist,r]:
   zV[r,rt,Client,CertificateTemplate] + zV[r,lb,Client,Therapist]
   + zV[r,rt,CertificateTemplate,Therapist] <= 2
 triangle1[Client,CertificateTemplate,Therapist,c]:
   zV[c,rt,Client,CertificateTemplate] + zV[c,lb,Client,Therapist]
   + zV[c,rt,CertificateTemplate,Therapist] <= 2
 triangle1[Client,CertificateTemplate,Instructor,r]:
   zV[r,rt,Client,CertificateTemplate] + zV[r,lb,Client,Instructor]
   + zV[r,rt,CertificateTemplate,Instructor] <= 2
 triangle1[Client,CertificateTemplate,Instructor,c]:
   zV[c,rt,Client,CertificateTemplate] + zV[c,lb,Client,Instructor]
   + zV[c,rt,CertificateTemplate,Instructor] <= 2
 triangle1[Client,Course,Role,r]: zV[r,rt,Client,Course]
   + zV[r,lb,Client,Role] + zV[r,rt,Course,Role] <= 2
 triangle1[Client,Course,Role,c]: zV[c,rt,Client,Course]
   + zV[c,lb,Client,Role] + zV[c,rt,Course,Role] <= 2
 triangle1[Client,Course,User,r]: zV[r,rt,Client,Course]
   + zV[r,lb,Client,User] + zV[r,rt,Course,User] <= 2
 triangle1[Client,Course,User,c]: zV[c,rt,Client,Course]
   + zV[c,lb,Client,User] + zV[c,rt,Course,User] <= 2
 triangle1[Client,Course,CourseSession,r]: zV[r,rt,Client,Course]
   + zV[r,lb,Client,CourseSession] + zV[r,rt,Course,CourseSession] <= 2
 triangle1[Client,Course,CourseSession,c]: zV[c,rt,Client,Course]
   + zV[c,lb,Client,CourseSession] + zV[c,rt,Course,CourseSession] <= 2
 triangle1[Client,Course,Clinic,r]: zV[r,rt,Client,Course]
   + zV[r,lb,Client,Clinic] + zV[r,rt,Course,Clinic] <= 2
 triangle1[Client,Course,Clinic,c]: zV[c,rt,Client,Course]
   + zV[c,lb,Client,Clinic] + zV[c,rt,Course,Clinic] <= 2
 triangle1[Client,Course,Location,r]: zV[r,rt,Client,Course]
   + zV[r,lb,Client,Location] + zV[r,rt,Course,Location] <= 2
 triangle1[Client,Course,Location,c]: zV[c,rt,Client,Course]
   + zV[c,lb,Client,Location] + zV[c,rt,Course,Location] <= 2
 triangle1[Client,Course,Therapist,r]: zV[r,rt,Client,Course]
   + zV[r,lb,Client,Therapist] + zV[r,rt,Course,Therapist] <= 2
 triangle1[Client,Course,Therapist,c]: zV[c,rt,Client,Course]
   + zV[c,lb,Client,Therapist] + zV[c,rt,Course,Therapist] <= 2
 triangle1[Client,Course,Instructor,r]: zV[r,rt,Client,Course]
   + zV[r,lb,Client,Instructor] + zV[r,rt,Course,Instructor] <= 2
 triangle1[Client,Course,Instructor,c]: zV[c,rt,Client,Course]
   + zV[c,lb,Client,Instructor] + zV[c,rt,Course,Instructor] <= 2
 triangle1[Client,Role,User,r]: zV[r,rt,Client,Role] + zV[r,lb,Client,User]
   + zV[r,rt,Role,User] <= 2
 triangle1[Client,Role,User,c]: zV[c,rt,Client,Role] + zV[c,lb,Client,User]
   + zV[c,rt,Role,User] <= 2
 triangle1[Client,Role,CourseSession,r]: zV[r,rt,Client,Role]
   + zV[r,lb,Client,CourseSession] + zV[r,rt,Role,CourseSession] <= 2
 triangle1[Client,Role,CourseSession,c]: zV[c,rt,Client,Role]
   + zV[c,lb,Client,CourseSession] + zV[c,rt,Role,CourseSession] <= 2
 triangle1[Client,Role,Clinic,r]: zV[r,rt,Client,Role]
   + zV[r,lb,Client,Clinic] + zV[r,rt,Role,Clinic] <= 2
 triangle1[Client,Role,Clinic,c]: zV[c,rt,Client,Role]
   + zV[c,lb,Client,Clinic] + zV[c,rt,Role,Clinic] <= 2
 triangle1[Client,Role,Location,r]: zV[r,rt,Client,Role]
   + zV[r,lb,Client,Location] + zV[r,rt,Role,Location] <= 2
 triangle1[Client,Role,Location,c]: zV[c,rt,Client,Role]
   + zV[c,lb,Client,Location] + zV[c,rt,Role,Location] <= 2
 triangle1[Client,Role,Therapist,r]: zV[r,rt,Client,Role]
   + zV[r,lb,Client,Therapist] + zV[r,rt,Role,Therapist] <= 2
 triangle1[Client,Role,Therapist,c]: zV[c,rt,Client,Role]
   + zV[c,lb,Client,Therapist] + zV[c,rt,Role,Therapist] <= 2
 triangle1[Client,Role,Instructor,r]: zV[r,rt,Client,Role]
   + zV[r,lb,Client,Instructor] + zV[r,rt,Role,Instructor] <= 2
 triangle1[Client,Role,Instructor,c]: zV[c,rt,Client,Role]
   + zV[c,lb,Client,Instructor] + zV[c,rt,Role,Instructor] <= 2
 triangle1[Client,User,CourseSession,r]: zV[r,rt,Client,User]
   + zV[r,lb,Client,CourseSession] + zV[r,rt,User,CourseSession] <= 2
 triangle1[Client,User,CourseSession,c]: zV[c,rt,Client,User]
   + zV[c,lb,Client,CourseSession] + zV[c,rt,User,CourseSession] <= 2
 triangle1[Client,User,Clinic,r]: zV[r,rt,Client,User]
   + zV[r,lb,Client,Clinic] + zV[r,rt,User,Clinic] <= 2
 triangle1[Client,User,Clinic,c]: zV[c,rt,Client,User]
   + zV[c,lb,Client,Clinic] + zV[c,rt,User,Clinic] <= 2
 triangle1[Client,User,Location,r]: zV[r,rt,Client,User]
   + zV[r,lb,Client,Location] + zV[r,rt,User,Location] <= 2
 triangle1[Client,User,Location,c]: zV[c,rt,Client,User]
   + zV[c,lb,Client,Location] + zV[c,rt,User,Location] <= 2
 triangle1[Client,User,Therapist,r]: zV[r,rt,Client,User]
   + zV[r,lb,Client,Therapist] + zV[r,rt,User,Therapist] <= 2
 triangle1[Client,User,Therapist,c]: zV[c,rt,Client,User]
   + zV[c,lb,Client,Therapist] + zV[c,rt,User,Therapist] <= 2
 triangle1[Client,User,Instructor,r]: zV[r,rt,Client,User]
   + zV[r,lb,Client,Instructor] + zV[r,rt,User,Instructor] <= 2
 triangle1[Client,User,Instructor,c]: zV[c,rt,Client,User]
   + zV[c,lb,Client,Instructor] + zV[c,rt,User,Instructor] <= 2
 triangle1[Client,CourseSession,Clinic,r]: zV[r,rt,Client,CourseSession]
   + zV[r,lb,Client,Clinic] + zV[r,rt,CourseSession,Clinic] <= 2
 triangle1[Client,CourseSession,Clinic,c]: zV[c,rt,Client,CourseSession]
   + zV[c,lb,Client,Clinic] + zV[c,rt,CourseSession,Clinic] <= 2
 triangle1[Client,CourseSession,Location,r]: zV[r,rt,Client,CourseSession]
   + zV[r,lb,Client,Location] + zV[r,rt,CourseSession,Location] <= 2
 triangle1[Client,CourseSession,Location,c]: zV[c,rt,Client,CourseSession]
   + zV[c,lb,Client,Location] + zV[c,rt,CourseSession,Location] <= 2
 triangle1[Client,CourseSession,Therapist,r]: zV[r,rt,Client,CourseSession]
   + zV[r,lb,Client,Therapist] + zV[r,rt,CourseSession,Therapist] <= 2
 triangle1[Client,CourseSession,Therapist,c]: zV[c,rt,Client,CourseSession]
   + zV[c,lb,Client,Therapist] + zV[c,rt,CourseSession,Therapist] <= 2
 triangle1[Client,CourseSession,Instructor,r]:
   zV[r,rt,Client,CourseSession] + zV[r,lb,Client,Instructor]
   + zV[r,rt,CourseSession,Instructor] <= 2
 triangle1[Client,CourseSession,Instructor,c]:
   zV[c,rt,Client,CourseSession] + zV[c,lb,Client,Instructor]
   + zV[c,rt,CourseSession,Instructor] <= 2
 triangle1[Client,Clinic,Location,r]: zV[r,rt,Client,Clinic]
   + zV[r,lb,Client,Location] + zV[r,rt,Clinic,Location] <= 2
 triangle1[Client,Clinic,Location,c]: zV[c,rt,Client,Clinic]
   + zV[c,lb,Client,Location] + zV[c,rt,Clinic,Location] <= 2
 triangle1[Client,Clinic,Therapist,r]: zV[r,rt,Client,Clinic]
   + zV[r,lb,Client,Therapist] + zV[r,rt,Clinic,Therapist] <= 2
 triangle1[Client,Clinic,Therapist,c]: zV[c,rt,Client,Clinic]
   + zV[c,lb,Client,Therapist] + zV[c,rt,Clinic,Therapist] <= 2
 triangle1[Client,Clinic,Instructor,r]: zV[r,rt,Client,Clinic]
   + zV[r,lb,Client,Instructor] + zV[r,rt,Clinic,Instructor] <= 2
 triangle1[Client,Clinic,Instructor,c]: zV[c,rt,Client,Clinic]
   + zV[c,lb,Client,Instructor] + zV[c,rt,Clinic,Instructor] <= 2
 triangle1[Client,Location,Therapist,r]: zV[r,rt,Client,Location]
   + zV[r,lb,Client,Therapist] + zV[r,rt,Location,Therapist] <= 2
 triangle1[Client,Location,Therapist,c]: zV[c,rt,Client,Location]
   + zV[c,lb,Client,Therapist] + zV[c,rt,Location,Therapist] <= 2
 triangle1[Client,Location,Instructor,r]: zV[r,rt,Client,Location]
   + zV[r,lb,Client,Instructor] + zV[r,rt,Location,Instructor] <= 2
 triangle1[Client,Location,Instructor,c]: zV[c,rt,Client,Location]
   + zV[c,lb,Client,Instructor] + zV[c,rt,Location,Instructor] <= 2
 triangle1[Client,Therapist,Instructor,r]: zV[r,rt,Client,Therapist]
   + zV[r,lb,Client,Instructor] + zV[r,rt,Therapist,Instructor] <= 2
 triangle1[Client,Therapist,Instructor,c]: zV[c,rt,Client,Therapist]
   + zV[c,lb,Client,Instructor] + zV[c,rt,Therapist,Instructor] <= 2
 triangle1[CertificateTemplate,Course,Role,r]:
   zV[r,rt,CertificateTemplate,Course] + zV[r,lb,CertificateTemplate,Role]
   + zV[r,rt,Course,Role] <= 2
 triangle1[CertificateTemplate,Course,Role,c]:
   zV[c,rt,CertificateTemplate,Course] + zV[c,lb,CertificateTemplate,Role]
   + zV[c,rt,Course,Role] <= 2
 triangle1[CertificateTemplate,Course,User,r]:
   zV[r,rt,CertificateTemplate,Course] + zV[r,lb,CertificateTemplate,User]
   + zV[r,rt,Course,User] <= 2
 triangle1[CertificateTemplate,Course,User,c]:
   zV[c,rt,CertificateTemplate,Course] + zV[c,lb,CertificateTemplate,User]
   + zV[c,rt,Course,User] <= 2
 triangle1[CertificateTemplate,Course,CourseSession,r]:
   zV[r,rt,CertificateTemplate,Course]
   + zV[r,lb,CertificateTemplate,CourseSession]
   + zV[r,rt,Course,CourseSession] <= 2
 triangle1[CertificateTemplate,Course,CourseSession,c]:
   zV[c,rt,CertificateTemplate,Course]
   + zV[c,lb,CertificateTemplate,CourseSession]
   + zV[c,rt,Course,CourseSession] <= 2
 triangle1[CertificateTemplate,Course,Clinic,r]:
   zV[r,rt,CertificateTemplate,Course]
   + zV[r,lb,CertificateTemplate,Clinic] + zV[r,rt,Course,Clinic] <= 2
 triangle1[CertificateTemplate,Course,Clinic,c]:
   zV[c,rt,CertificateTemplate,Course]
   + zV[c,lb,CertificateTemplate,Clinic] + zV[c,rt,Course,Clinic] <= 2
 triangle1[CertificateTemplate,Course,Location,r]:
   zV[r,rt,CertificateTemplate,Course]
   + zV[r,lb,CertificateTemplate,Location] + zV[r,rt,Course,Location] <= 2
 triangle1[CertificateTemplate,Course,Location,c]:
   zV[c,rt,CertificateTemplate,Course]
   + zV[c,lb,CertificateTemplate,Location] + zV[c,rt,Course,Location] <= 2
 triangle1[CertificateTemplate,Course,Therapist,r]:
   zV[r,rt,CertificateTemplate,Course]
   + zV[r,lb,CertificateTemplate,Therapist] + zV[r,rt,Course,Therapist]
   <= 2
 triangle1[CertificateTemplate,Course,Therapist,c]:
   zV[c,rt,CertificateTemplate,Course]
   + zV[c,lb,CertificateTemplate,Therapist] + zV[c,rt,Course,Therapist]
   <= 2
 triangle1[CertificateTemplate,Course,Instructor,r]:
   zV[r,rt,CertificateTemplate,Course]
   + zV[r,lb,CertificateTemplate,Instructor] + zV[r,rt,Course,Instructor]
   <= 2
 triangle1[CertificateTemplate,Course,Instructor,c]:
   zV[c,rt,CertificateTemplate,Course]
   + zV[c,lb,CertificateTemplate,Instructor] + zV[c,rt,Course,Instructor]
   <= 2
 triangle1[CertificateTemplate,Role,User,r]:
   zV[r,rt,CertificateTemplate,Role] + zV[r,lb,CertificateTemplate,User]
   + zV[r,rt,Role,User] <= 2
 triangle1[CertificateTemplate,Role,User,c]:
   zV[c,rt,CertificateTemplate,Role] + zV[c,lb,CertificateTemplate,User]
   + zV[c,rt,Role,User] <= 2
 triangle1[CertificateTemplate,Role,CourseSession,r]:
   zV[r,rt,CertificateTemplate,Role]
   + zV[r,lb,CertificateTemplate,CourseSession]
   + zV[r,rt,Role,CourseSession] <= 2
 triangle1[CertificateTemplate,Role,CourseSession,c]:
   zV[c,rt,CertificateTemplate,Role]
   + zV[c,lb,CertificateTemplate,CourseSession]
   + zV[c,rt,Role,CourseSession] <= 2
 triangle1[CertificateTemplate,Role,Clinic,r]:
   zV[r,rt,CertificateTemplate,Role] + zV[r,lb,CertificateTemplate,Clinic]
   + zV[r,rt,Role,Clinic] <= 2
 triangle1[CertificateTemplate,Role,Clinic,c]:
   zV[c,rt,CertificateTemplate,Role] + zV[c,lb,CertificateTemplate,Clinic]
   + zV[c,rt,Role,Clinic] <= 2
 triangle1[CertificateTemplate,Role,Location,r]:
   zV[r,rt,CertificateTemplate,Role]
   + zV[r,lb,CertificateTemplate,Location] + zV[r,rt,Role,Location] <= 2
 triangle1[CertificateTemplate,Role,Location,c]:
   zV[c,rt,CertificateTemplate,Role]
   + zV[c,lb,CertificateTemplate,Location] + zV[c,rt,Role,Location] <= 2
 triangle1[CertificateTemplate,Role,Therapist,r]:
   zV[r,rt,CertificateTemplate,Role]
   + zV[r,lb,CertificateTemplate,Therapist] + zV[r,rt,Role,Therapist] <= 2
 triangle1[CertificateTemplate,Role,Therapist,c]:
   zV[c,rt,CertificateTemplate,Role]
   + zV[c,lb,CertificateTemplate,Therapist] + zV[c,rt,Role,Therapist] <= 2
 triangle1[CertificateTemplate,Role,Instructor,r]:
   zV[r,rt,CertificateTemplate,Role]
   + zV[r,lb,CertificateTemplate,Instructor] + zV[r,rt,Role,Instructor]
   <= 2
 triangle1[CertificateTemplate,Role,Instructor,c]:
   zV[c,rt,CertificateTemplate,Role]
   + zV[c,lb,CertificateTemplate,Instructor] + zV[c,rt,Role,Instructor]
   <= 2
 triangle1[CertificateTemplate,User,CourseSession,r]:
   zV[r,rt,CertificateTemplate,User]
   + zV[r,lb,CertificateTemplate,CourseSession]
   + zV[r,rt,User,CourseSession] <= 2
 triangle1[CertificateTemplate,User,CourseSession,c]:
   zV[c,rt,CertificateTemplate,User]
   + zV[c,lb,CertificateTemplate,CourseSession]
   + zV[c,rt,User,CourseSession] <= 2
 triangle1[CertificateTemplate,User,Clinic,r]:
   zV[r,rt,CertificateTemplate,User] + zV[r,lb,CertificateTemplate,Clinic]
   + zV[r,rt,User,Clinic] <= 2
 triangle1[CertificateTemplate,User,Clinic,c]:
   zV[c,rt,CertificateTemplate,User] + zV[c,lb,CertificateTemplate,Clinic]
   + zV[c,rt,User,Clinic] <= 2
 triangle1[CertificateTemplate,User,Location,r]:
   zV[r,rt,CertificateTemplate,User]
   + zV[r,lb,CertificateTemplate,Location] + zV[r,rt,User,Location] <= 2
 triangle1[CertificateTemplate,User,Location,c]:
   zV[c,rt,CertificateTemplate,User]
   + zV[c,lb,CertificateTemplate,Location] + zV[c,rt,User,Location] <= 2
 triangle1[CertificateTemplate,User,Therapist,r]:
   zV[r,rt,CertificateTemplate,User]
   + zV[r,lb,CertificateTemplate,Therapist] + zV[r,rt,User,Therapist] <= 2
 triangle1[CertificateTemplate,User,Therapist,c]:
   zV[c,rt,CertificateTemplate,User]
   + zV[c,lb,CertificateTemplate,Therapist] + zV[c,rt,User,Therapist] <= 2
 triangle1[CertificateTemplate,User,Instructor,r]:
   zV[r,rt,CertificateTemplate,User]
   + zV[r,lb,CertificateTemplate,Instructor] + zV[r,rt,User,Instructor]
   <= 2
 triangle1[CertificateTemplate,User,Instructor,c]:
   zV[c,rt,CertificateTemplate,User]
   + zV[c,lb,CertificateTemplate,Instructor] + zV[c,rt,User,Instructor]
   <= 2
 triangle1[CertificateTemplate,CourseSession,Clinic,r]:
   zV[r,rt,CertificateTemplate,CourseSession]
   + zV[r,lb,CertificateTemplate,Clinic] + zV[r,rt,CourseSession,Clinic]
   <= 2
 triangle1[CertificateTemplate,CourseSession,Clinic,c]:
   zV[c,rt,CertificateTemplate,CourseSession]
   + zV[c,lb,CertificateTemplate,Clinic] + zV[c,rt,CourseSession,Clinic]
   <= 2
 triangle1[CertificateTemplate,CourseSession,Location,r]:
   zV[r,rt,CertificateTemplate,CourseSession]
   + zV[r,lb,CertificateTemplate,Location]
   + zV[r,rt,CourseSession,Location] <= 2
 triangle1[CertificateTemplate,CourseSession,Location,c]:
   zV[c,rt,CertificateTemplate,CourseSession]
   + zV[c,lb,CertificateTemplate,Location]
   + zV[c,rt,CourseSession,Location] <= 2
 triangle1[CertificateTemplate,CourseSession,Therapist,r]:
   zV[r,rt,CertificateTemplate,CourseSession]
   + zV[r,lb,CertificateTemplate,Therapist]
   + zV[r,rt,CourseSession,Therapist] <= 2
 triangle1[CertificateTemplate,CourseSession,Therapist,c]:
   zV[c,rt,CertificateTemplate,CourseSession]
   + zV[c,lb,CertificateTemplate,Therapist]
   + zV[c,rt,CourseSession,Therapist] <= 2
 triangle1[CertificateTemplate,CourseSession,Instructor,r]:
   zV[r,rt,CertificateTemplate,CourseSession]
   + zV[r,lb,CertificateTemplate,Instructor]
   + zV[r,rt,CourseSession,Instructor] <= 2
 triangle1[CertificateTemplate,CourseSession,Instructor,c]:
   zV[c,rt,CertificateTemplate,CourseSession]
   + zV[c,lb,CertificateTemplate,Instructor]
   + zV[c,rt,CourseSession,Instructor] <= 2
 triangle1[CertificateTemplate,Clinic,Location,r]:
   zV[r,rt,CertificateTemplate,Clinic]
   + zV[r,lb,CertificateTemplate,Location] + zV[r,rt,Clinic,Location] <= 2
 triangle1[CertificateTemplate,Clinic,Location,c]:
   zV[c,rt,CertificateTemplate,Clinic]
   + zV[c,lb,CertificateTemplate,Location] + zV[c,rt,Clinic,Location] <= 2
 triangle1[CertificateTemplate,Clinic,Therapist,r]:
   zV[r,rt,CertificateTemplate,Clinic]
   + zV[r,lb,CertificateTemplate,Therapist] + zV[r,rt,Clinic,Therapist]
   <= 2
 triangle1[CertificateTemplate,Clinic,Therapist,c]:
   zV[c,rt,CertificateTemplate,Clinic]
   + zV[c,lb,CertificateTemplate,Therapist] + zV[c,rt,Clinic,Therapist]
   <= 2
 triangle1[CertificateTemplate,Clinic,Instructor,r]:
   zV[r,rt,CertificateTemplate,Clinic]
   + zV[r,lb,CertificateTemplate,Instructor] + zV[r,rt,Clinic,Instructor]
   <= 2
 triangle1[CertificateTemplate,Clinic,Instructor,c]:
   zV[c,rt,CertificateTemplate,Clinic]
   + zV[c,lb,CertificateTemplate,Instructor] + zV[c,rt,Clinic,Instructor]
   <= 2
 triangle1[CertificateTemplate,Location,Therapist,r]:
   zV[r,rt,CertificateTemplate,Location]
   + zV[r,lb,CertificateTemplate,Therapist] + zV[r,rt,Location,Therapist]
   <= 2
 triangle1[CertificateTemplate,Location,Therapist,c]:
   zV[c,rt,CertificateTemplate,Location]
   + zV[c,lb,CertificateTemplate,Therapist] + zV[c,rt,Location,Therapist]
   <= 2
 triangle1[CertificateTemplate,Location,Instructor,r]:
   zV[r,rt,CertificateTemplate,Location]
   + zV[r,lb,CertificateTemplate,Instructor] + zV[r,rt,Location,Instructor]
   <= 2
 triangle1[CertificateTemplate,Location,Instructor,c]:
   zV[c,rt,CertificateTemplate,Location]
   + zV[c,lb,CertificateTemplate,Instructor] + zV[c,rt,Location,Instructor]
   <= 2
 triangle1[CertificateTemplate,Therapist,Instructor,r]:
   zV[r,rt,CertificateTemplate,Therapist]
   + zV[r,lb,CertificateTemplate,Instructor]
   + zV[r,rt,Therapist,Instructor] <= 2
 triangle1[CertificateTemplate,Therapist,Instructor,c]:
   zV[c,rt,CertificateTemplate,Therapist]
   + zV[c,lb,CertificateTemplate,Instructor]
   + zV[c,rt,Therapist,Instructor] <= 2
 triangle1[Course,Role,User,r]: zV[r,rt,Course,Role] + zV[r,lb,Course,User]
   + zV[r,rt,Role,User] <= 2
 triangle1[Course,Role,User,c]: zV[c,rt,Course,Role] + zV[c,lb,Course,User]
   + zV[c,rt,Role,User] <= 2
 triangle1[Course,Role,CourseSession,r]: zV[r,rt,Course,Role]
   + zV[r,lb,Course,CourseSession] + zV[r,rt,Role,CourseSession] <= 2
 triangle1[Course,Role,CourseSession,c]: zV[c,rt,Course,Role]
   + zV[c,lb,Course,CourseSession] + zV[c,rt,Role,CourseSession] <= 2
 triangle1[Course,Role,Clinic,r]: zV[r,rt,Course,Role]
   + zV[r,lb,Course,Clinic] + zV[r,rt,Role,Clinic] <= 2
 triangle1[Course,Role,Clinic,c]: zV[c,rt,Course,Role]
   + zV[c,lb,Course,Clinic] + zV[c,rt,Role,Clinic] <= 2
 triangle1[Course,Role,Location,r]: zV[r,rt,Course,Role]
   + zV[r,lb,Course,Location] + zV[r,rt,Role,Location] <= 2
 triangle1[Course,Role,Location,c]: zV[c,rt,Course,Role]
   + zV[c,lb,Course,Location] + zV[c,rt,Role,Location] <= 2
 triangle1[Course,Role,Therapist,r]: zV[r,rt,Course,Role]
   + zV[r,lb,Course,Therapist] + zV[r,rt,Role,Therapist] <= 2
 triangle1[Course,Role,Therapist,c]: zV[c,rt,Course,Role]
   + zV[c,lb,Course,Therapist] + zV[c,rt,Role,Therapist] <= 2
 triangle1[Course,Role,Instructor,r]: zV[r,rt,Course,Role]
   + zV[r,lb,Course,Instructor] + zV[r,rt,Role,Instructor] <= 2
 triangle1[Course,Role,Instructor,c]: zV[c,rt,Course,Role]
   + zV[c,lb,Course,Instructor] + zV[c,rt,Role,Instructor] <= 2
 triangle1[Course,User,CourseSession,r]: zV[r,rt,Course,User]
   + zV[r,lb,Course,CourseSession] + zV[r,rt,User,CourseSession] <= 2
 triangle1[Course,User,CourseSession,c]: zV[c,rt,Course,User]
   + zV[c,lb,Course,CourseSession] + zV[c,rt,User,CourseSession] <= 2
 triangle1[Course,User,Clinic,r]: zV[r,rt,Course,User]
   + zV[r,lb,Course,Clinic] + zV[r,rt,User,Clinic] <= 2
 triangle1[Course,User,Clinic,c]: zV[c,rt,Course,User]
   + zV[c,lb,Course,Clinic] + zV[c,rt,User,Clinic] <= 2
 triangle1[Course,User,Location,r]: zV[r,rt,Course,User]
   + zV[r,lb,Course,Location] + zV[r,rt,User,Location] <= 2
 triangle1[Course,User,Location,c]: zV[c,rt,Course,User]
   + zV[c,lb,Course,Location] + zV[c,rt,User,Location] <= 2
 triangle1[Course,User,Therapist,r]: zV[r,rt,Course,User]
   + zV[r,lb,Course,Therapist] + zV[r,rt,User,Therapist] <= 2
 triangle1[Course,User,Therapist,c]: zV[c,rt,Course,User]
   + zV[c,lb,Course,Therapist] + zV[c,rt,User,Therapist] <= 2
 triangle1[Course,User,Instructor,r]: zV[r,rt,Course,User]
   + zV[r,lb,Course,Instructor] + zV[r,rt,User,Instructor] <= 2
 triangle1[Course,User,Instructor,c]: zV[c,rt,Course,User]
   + zV[c,lb,Course,Instructor] + zV[c,rt,User,Instructor] <= 2
 triangle1[Course,CourseSession,Clinic,r]: zV[r,rt,Course,CourseSession]
   + zV[r,lb,Course,Clinic] + zV[r,rt,CourseSession,Clinic] <= 2
 triangle1[Course,CourseSession,Clinic,c]: zV[c,rt,Course,CourseSession]
   + zV[c,lb,Course,Clinic] + zV[c,rt,CourseSession,Clinic] <= 2
 triangle1[Course,CourseSession,Location,r]: zV[r,rt,Course,CourseSession]
   + zV[r,lb,Course,Location] + zV[r,rt,CourseSession,Location] <= 2
 triangle1[Course,CourseSession,Location,c]: zV[c,rt,Course,CourseSession]
   + zV[c,lb,Course,Location] + zV[c,rt,CourseSession,Location] <= 2
 triangle1[Course,CourseSession,Therapist,r]: zV[r,rt,Course,CourseSession]
   + zV[r,lb,Course,Therapist] + zV[r,rt,CourseSession,Therapist] <= 2
 triangle1[Course,CourseSession,Therapist,c]: zV[c,rt,Course,CourseSession]
   + zV[c,lb,Course,Therapist] + zV[c,rt,CourseSession,Therapist] <= 2
 triangle1[Course,CourseSession,Instructor,r]:
   zV[r,rt,Course,CourseSession] + zV[r,lb,Course,Instructor]
   + zV[r,rt,CourseSession,Instructor] <= 2
 triangle1[Course,CourseSession,Instructor,c]:
   zV[c,rt,Course,CourseSession] + zV[c,lb,Course,Instructor]
   + zV[c,rt,CourseSession,Instructor] <= 2
 triangle1[Course,Clinic,Location,r]: zV[r,rt,Course,Clinic]
   + zV[r,lb,Course,Location] + zV[r,rt,Clinic,Location] <= 2
 triangle1[Course,Clinic,Location,c]: zV[c,rt,Course,Clinic]
   + zV[c,lb,Course,Location] + zV[c,rt,Clinic,Location] <= 2
 triangle1[Course,Clinic,Therapist,r]: zV[r,rt,Course,Clinic]
   + zV[r,lb,Course,Therapist] + zV[r,rt,Clinic,Therapist] <= 2
 triangle1[Course,Clinic,Therapist,c]: zV[c,rt,Course,Clinic]
   + zV[c,lb,Course,Therapist] + zV[c,rt,Clinic,Therapist] <= 2
 triangle1[Course,Clinic,Instructor,r]: zV[r,rt,Course,Clinic]
   + zV[r,lb,Course,Instructor] + zV[r,rt,Clinic,Instructor] <= 2
 triangle1[Course,Clinic,Instructor,c]: zV[c,rt,Course,Clinic]
   + zV[c,lb,Course,Instructor] + zV[c,rt,Clinic,Instructor] <= 2
 triangle1[Course,Location,Therapist,r]: zV[r,rt,Course,Location]
   + zV[r,lb,Course,Therapist] + zV[r,rt,Location,Therapist] <= 2
 triangle1[Course,Location,Therapist,c]: zV[c,rt,Course,Location]
   + zV[c,lb,Course,Therapist] + zV[c,rt,Location,Therapist] <= 2
 triangle1[Course,Location,Instructor,r]: zV[r,rt,Course,Location]
   + zV[r,lb,Course,Instructor] + zV[r,rt,Location,Instructor] <= 2
 triangle1[Course,Location,Instructor,c]: zV[c,rt,Course,Location]
   + zV[c,lb,Course,Instructor] + zV[c,rt,Location,Instructor] <= 2
 triangle1[Course,Therapist,Instructor,r]: zV[r,rt,Course,Therapist]
   + zV[r,lb,Course,Instructor] + zV[r,rt,Therapist,Instructor] <= 2
 triangle1[Course,Therapist,Instructor,c]: zV[c,rt,Course,Therapist]
   + zV[c,lb,Course,Instructor] + zV[c,rt,Therapist,Instructor] <= 2
 triangle1[Role,User,CourseSession,r]: zV[r,rt,Role,User]
   + zV[r,lb,Role,CourseSession] + zV[r,rt,User,CourseSession] <= 2
 triangle1[Role,User,CourseSession,c]: zV[c,rt,Role,User]
   + zV[c,lb,Role,CourseSession] + zV[c,rt,User,CourseSession] <= 2
 triangle1[Role,User,Clinic,r]: zV[r,rt,Role,User] + zV[r,lb,Role,Clinic]
   + zV[r,rt,User,Clinic] <= 2
 triangle1[Role,User,Clinic,c]: zV[c,rt,Role,User] + zV[c,lb,Role,Clinic]
   + zV[c,rt,User,Clinic] <= 2
 triangle1[Role,User,Location,r]: zV[r,rt,Role,User]
   + zV[r,lb,Role,Location] + zV[r,rt,User,Location] <= 2
 triangle1[Role,User,Location,c]: zV[c,rt,Role,User]
   + zV[c,lb,Role,Location] + zV[c,rt,User,Location] <= 2
 triangle1[Role,User,Therapist,r]: zV[r,rt,Role,User]
   + zV[r,lb,Role,Therapist] + zV[r,rt,User,Therapist] <= 2
 triangle1[Role,User,Therapist,c]: zV[c,rt,Role,User]
   + zV[c,lb,Role,Therapist] + zV[c,rt,User,Therapist] <= 2
 triangle1[Role,User,Instructor,r]: zV[r,rt,Role,User]
   + zV[r,lb,Role,Instructor] + zV[r,rt,User,Instructor] <= 2
 triangle1[Role,User,Instructor,c]: zV[c,rt,Role,User]
   + zV[c,lb,Role,Instructor] + zV[c,rt,User,Instructor] <= 2
 triangle1[Role,CourseSession,Clinic,r]: zV[r,rt,Role,CourseSession]
   + zV[r,lb,Role,Clinic] + zV[r,rt,CourseSession,Clinic] <= 2
 triangle1[Role,CourseSession,Clinic,c]: zV[c,rt,Role,CourseSession]
   + zV[c,lb,Role,Clinic] + zV[c,rt,CourseSession,Clinic] <= 2
 triangle1[Role,CourseSession,Location,r]: zV[r,rt,Role,CourseSession]
   + zV[r,lb,Role,Location] + zV[r,rt,CourseSession,Location] <= 2
 triangle1[Role,CourseSession,Location,c]: zV[c,rt,Role,CourseSession]
   + zV[c,lb,Role,Location] + zV[c,rt,CourseSession,Location] <= 2
 triangle1[Role,CourseSession,Therapist,r]: zV[r,rt,Role,CourseSession]
   + zV[r,lb,Role,Therapist] + zV[r,rt,CourseSession,Therapist] <= 2
 triangle1[Role,CourseSession,Therapist,c]: zV[c,rt,Role,CourseSession]
   + zV[c,lb,Role,Therapist] + zV[c,rt,CourseSession,Therapist] <= 2
 triangle1[Role,CourseSession,Instructor,r]: zV[r,rt,Role,CourseSession]
   + zV[r,lb,Role,Instructor] + zV[r,rt,CourseSession,Instructor] <= 2
 triangle1[Role,CourseSession,Instructor,c]: zV[c,rt,Role,CourseSession]
   + zV[c,lb,Role,Instructor] + zV[c,rt,CourseSession,Instructor] <= 2
 triangle1[Role,Clinic,Location,r]: zV[r,rt,Role,Clinic]
   + zV[r,lb,Role,Location] + zV[r,rt,Clinic,Location] <= 2
 triangle1[Role,Clinic,Location,c]: zV[c,rt,Role,Clinic]
   + zV[c,lb,Role,Location] + zV[c,rt,Clinic,Location] <= 2
 triangle1[Role,Clinic,Therapist,r]: zV[r,rt,Role,Clinic]
   + zV[r,lb,Role,Therapist] + zV[r,rt,Clinic,Therapist] <= 2
 triangle1[Role,Clinic,Therapist,c]: zV[c,rt,Role,Clinic]
   + zV[c,lb,Role,Therapist] + zV[c,rt,Clinic,Therapist] <= 2
 triangle1[Role,Clinic,Instructor,r]: zV[r,rt,Role,Clinic]
   + zV[r,lb,Role,Instructor] + zV[r,rt,Clinic,Instructor] <= 2
 triangle1[Role,Clinic,Instructor,c]: zV[c,rt,Role,Clinic]
   + zV[c,lb,Role,Instructor] + zV[c,rt,Clinic,Instructor] <= 2
 triangle1[Role,Location,Therapist,r]: zV[r,rt,Role,Location]
   + zV[r,lb,Role,Therapist] + zV[r,rt,Location,Therapist] <= 2
 triangle1[Role,Location,Therapist,c]: zV[c,rt,Role,Location]
   + zV[c,lb,Role,Therapist] + zV[c,rt,Location,Therapist] <= 2
 triangle1[Role,Location,Instructor,r]: zV[r,rt,Role,Location]
   + zV[r,lb,Role,Instructor] + zV[r,rt,Location,Instructor] <= 2
 triangle1[Role,Location,Instructor,c]: zV[c,rt,Role,Location]
   + zV[c,lb,Role,Instructor] + zV[c,rt,Location,Instructor] <= 2
 triangle1[Role,Therapist,Instructor,r]: zV[r,rt,Role,Therapist]
   + zV[r,lb,Role,Instructor] + zV[r,rt,Therapist,Instructor] <= 2
 triangle1[Role,Therapist,Instructor,c]: zV[c,rt,Role,Therapist]
   + zV[c,lb,Role,Instructor] + zV[c,rt,Therapist,Instructor] <= 2
 triangle1[User,CourseSession,Clinic,r]: zV[r,rt,User,CourseSession]
   + zV[r,lb,User,Clinic] + zV[r,rt,CourseSession,Clinic] <= 2
 triangle1[User,CourseSession,Clinic,c]: zV[c,rt,User,CourseSession]
   + zV[c,lb,User,Clinic] + zV[c,rt,CourseSession,Clinic] <= 2
 triangle1[User,CourseSession,Location,r]: zV[r,rt,User,CourseSession]
   + zV[r,lb,User,Location] + zV[r,rt,CourseSession,Location] <= 2
 triangle1[User,CourseSession,Location,c]: zV[c,rt,User,CourseSession]
   + zV[c,lb,User,Location] + zV[c,rt,CourseSession,Location] <= 2
 triangle1[User,CourseSession,Therapist,r]: zV[r,rt,User,CourseSession]
   + zV[r,lb,User,Therapist] + zV[r,rt,CourseSession,Therapist] <= 2
 triangle1[User,CourseSession,Therapist,c]: zV[c,rt,User,CourseSession]
   + zV[c,lb,User,Therapist] + zV[c,rt,CourseSession,Therapist] <= 2
 triangle1[User,CourseSession,Instructor,r]: zV[r,rt,User,CourseSession]
   + zV[r,lb,User,Instructor] + zV[r,rt,CourseSession,Instructor] <= 2
 triangle1[User,CourseSession,Instructor,c]: zV[c,rt,User,CourseSession]
   + zV[c,lb,User,Instructor] + zV[c,rt,CourseSession,Instructor] <= 2
 triangle1[User,Clinic,Location,r]: zV[r,rt,User,Clinic]
   + zV[r,lb,User,Location] + zV[r,rt,Clinic,Location] <= 2
 triangle1[User,Clinic,Location,c]: zV[c,rt,User,Clinic]
   + zV[c,lb,User,Location] + zV[c,rt,Clinic,Location] <= 2
 triangle1[User,Clinic,Therapist,r]: zV[r,rt,User,Clinic]
   + zV[r,lb,User,Therapist] + zV[r,rt,Clinic,Therapist] <= 2
 triangle1[User,Clinic,Therapist,c]: zV[c,rt,User,Clinic]
   + zV[c,lb,User,Therapist] + zV[c,rt,Clinic,Therapist] <= 2
 triangle1[User,Clinic,Instructor,r]: zV[r,rt,User,Clinic]
   + zV[r,lb,User,Instructor] + zV[r,rt,Clinic,Instructor] <= 2
 triangle1[User,Clinic,Instructor,c]: zV[c,rt,User,Clinic]
   + zV[c,lb,User,Instructor] + zV[c,rt,Clinic,Instructor] <= 2
 triangle1[User,Location,Therapist,r]: zV[r,rt,User,Location]
   + zV[r,lb,User,Therapist] + zV[r,rt,Location,Therapist] <= 2
 triangle1[User,Location,Therapist,c]: zV[c,rt,User,Location]
   + zV[c,lb,User,Therapist] + zV[c,rt,Location,Therapist] <= 2
 triangle1[User,Location,Instructor,r]: zV[r,rt,User,Location]
   + zV[r,lb,User,Instructor] + zV[r,rt,Location,Instructor] <= 2
 triangle1[User,Location,Instructor,c]: zV[c,rt,User,Location]
   + zV[c,lb,User,Instructor] + zV[c,rt,Location,Instructor] <= 2
 triangle1[User,Therapist,Instructor,r]: zV[r,rt,User,Therapist]
   + zV[r,lb,User,Instructor] + zV[r,rt,Therapist,Instructor] <= 2
 triangle1[User,Therapist,Instructor,c]: zV[c,rt,User,Therapist]
   + zV[c,lb,User,Instructor] + zV[c,rt,Therapist,Instructor] <= 2
 triangle1[CourseSession,Clinic,Location,r]: zV[r,rt,CourseSession,Clinic]
   + zV[r,lb,CourseSession,Location] + zV[r,rt,Clinic,Location] <= 2
 triangle1[CourseSession,Clinic,Location,c]: zV[c,rt,CourseSession,Clinic]
   + zV[c,lb,CourseSession,Location] + zV[c,rt,Clinic,Location] <= 2
 triangle1[CourseSession,Clinic,Therapist,r]: zV[r,rt,CourseSession,Clinic]
   + zV[r,lb,CourseSession,Therapist] + zV[r,rt,Clinic,Therapist] <= 2
 triangle1[CourseSession,Clinic,Therapist,c]: zV[c,rt,CourseSession,Clinic]
   + zV[c,lb,CourseSession,Therapist] + zV[c,rt,Clinic,Therapist] <= 2
 triangle1[CourseSession,Clinic,Instructor,r]:
   zV[r,rt,CourseSession,Clinic] + zV[r,lb,CourseSession,Instructor]
   + zV[r,rt,Clinic,Instructor] <= 2
 triangle1[CourseSession,Clinic,Instructor,c]:
   zV[c,rt,CourseSession,Clinic] + zV[c,lb,CourseSession,Instructor]
   + zV[c,rt,Clinic,Instructor] <= 2
 triangle1[CourseSession,Location,Therapist,r]:
   zV[r,rt,CourseSession,Location] + zV[r,lb,CourseSession,Therapist]
   + zV[r,rt,Location,Therapist] <= 2
 triangle1[CourseSession,Location,Therapist,c]:
   zV[c,rt,CourseSession,Location] + zV[c,lb,CourseSession,Therapist]
   + zV[c,rt,Location,Therapist] <= 2
 triangle1[CourseSession,Location,Instructor,r]:
   zV[r,rt,CourseSession,Location] + zV[r,lb,CourseSession,Instructor]
   + zV[r,rt,Location,Instructor] <= 2
 triangle1[CourseSession,Location,Instructor,c]:
   zV[c,rt,CourseSession,Location] + zV[c,lb,CourseSession,Instructor]
   + zV[c,rt,Location,Instructor] <= 2
 triangle1[CourseSession,Therapist,Instructor,r]:
   zV[r,rt,CourseSession,Therapist] + zV[r,lb,CourseSession,Instructor]
   + zV[r,rt,Therapist,Instructor] <= 2
 triangle1[CourseSession,Therapist,Instructor,c]:
   zV[c,rt,CourseSession,Therapist] + zV[c,lb,CourseSession,Instructor]
   + zV[c,rt,Therapist,Instructor] <= 2
 triangle1[Clinic,Location,Therapist,r]: zV[r,rt,Clinic,Location]
   + zV[r,lb,Clinic,Therapist] + zV[r,rt,Location,Therapist] <= 2
 triangle1[Clinic,Location,Therapist,c]: zV[c,rt,Clinic,Location]
   + zV[c,lb,Clinic,Therapist] + zV[c,rt,Location,Therapist] <= 2
 triangle1[Clinic,Location,Instructor,r]: zV[r,rt,Clinic,Location]
   + zV[r,lb,Clinic,Instructor] + zV[r,rt,Location,Instructor] <= 2
 triangle1[Clinic,Location,Instructor,c]: zV[c,rt,Clinic,Location]
   + zV[c,lb,Clinic,Instructor] + zV[c,rt,Location,Instructor] <= 2
 triangle1[Clinic,Therapist,Instructor,r]: zV[r,rt,Clinic,Therapist]
   + zV[r,lb,Clinic,Instructor] + zV[r,rt,Therapist,Instructor] <= 2
 triangle1[Clinic,Therapist,Instructor,c]: zV[c,rt,Clinic,Therapist]
   + zV[c,lb,Clinic,Instructor] + zV[c,rt,Therapist,Instructor] <= 2
 triangle1[Location,Therapist,Instructor,r]: zV[r,rt,Location,Therapist]
   + zV[r,lb,Location,Instructor] + zV[r,rt,Therapist,Instructor] <= 2
 triangle1[Location,Therapist,Instructor,c]: zV[c,rt,Location,Therapist]
   + zV[c,lb,Location,Instructor] + zV[c,rt,Therapist,Instructor] <= 2
 triangle2[Client,CertificateTemplate,Course,r]:
   zV[r,lb,Client,CertificateTemplate] + zV[r,rt,Client,Course]
   + zV[r,lb,CertificateTemplate,Course] <= 2
 triangle2[Client,CertificateTemplate,Course,c]:
   zV[c,lb,Client,CertificateTemplate] + zV[c,rt,Client,Course]
   + zV[c,lb,CertificateTemplate,Course] <= 2
 triangle2[Client,CertificateTemplate,Role,r]:
   zV[r,lb,Client,CertificateTemplate] + zV[r,rt,Client,Role]
   + zV[r,lb,CertificateTemplate,Role] <= 2
 triangle2[Client,CertificateTemplate,Role,c]:
   zV[c,lb,Client,CertificateTemplate] + zV[c,rt,Client,Role]
   + zV[c,lb,CertificateTemplate,Role] <= 2
 triangle2[Client,CertificateTemplate,User,r]:
   zV[r,lb,Client,CertificateTemplate] + zV[r,rt,Client,User]
   + zV[r,lb,CertificateTemplate,User] <= 2
 triangle2[Client,CertificateTemplate,User,c]:
   zV[c,lb,Client,CertificateTemplate] + zV[c,rt,Client,User]
   + zV[c,lb,CertificateTemplate,User] <= 2
 triangle2[Client,CertificateTemplate,CourseSession,r]:
   zV[r,lb,Client,CertificateTemplate] + zV[r,rt,Client,CourseSession]
   + zV[r,lb,CertificateTemplate,CourseSession] <= 2
 triangle2[Client,CertificateTemplate,CourseSession,c]:
   zV[c,lb,Client,CertificateTemplate] + zV[c,rt,Client,CourseSession]
   + zV[c,lb,CertificateTemplate,CourseSession] <= 2
 triangle2[Client,CertificateTemplate,Clinic,r]:
   zV[r,lb,Client,CertificateTemplate] + zV[r,rt,Client,Clinic]
   + zV[r,lb,CertificateTemplate,Clinic] <= 2
 triangle2[Client,CertificateTemplate,Clinic,c]:
   zV[c,lb,Client,CertificateTemplate] + zV[c,rt,Client,Clinic]
   + zV[c,lb,CertificateTemplate,Clinic] <= 2
 triangle2[Client,CertificateTemplate,Location,r]:
   zV[r,lb,Client,CertificateTemplate] + zV[r,rt,Client,Location]
   + zV[r,lb,CertificateTemplate,Location] <= 2
 triangle2[Client,CertificateTemplate,Location,c]:
   zV[c,lb,Client,CertificateTemplate] + zV[c,rt,Client,Location]
   + zV[c,lb,CertificateTemplate,Location] <= 2
 triangle2[Client,CertificateTemplate,Therapist,r]:
   zV[r,lb,Client,CertificateTemplate] + zV[r,rt,Client,Therapist]
   + zV[r,lb,CertificateTemplate,Therapist] <= 2
 triangle2[Client,CertificateTemplate,Therapist,c]:
   zV[c,lb,Client,CertificateTemplate] + zV[c,rt,Client,Therapist]
   + zV[c,lb,CertificateTemplate,Therapist] <= 2
 triangle2[Client,CertificateTemplate,Instructor,r]:
   zV[r,lb,Client,CertificateTemplate] + zV[r,rt,Client,Instructor]
   + zV[r,lb,CertificateTemplate,Instructor] <= 2
 triangle2[Client,CertificateTemplate,Instructor,c]:
   zV[c,lb,Client,CertificateTemplate] + zV[c,rt,Client,Instructor]
   + zV[c,lb,CertificateTemplate,Instructor] <= 2
 triangle2[Client,Course,Role,r]: zV[r,lb,Client,Course]
   + zV[r,rt,Client,Role] + zV[r,lb,Course,Role] <= 2
 triangle2[Client,Course,Role,c]: zV[c,lb,Client,Course]
   + zV[c,rt,Client,Role] + zV[c,lb,Course,Role] <= 2
 triangle2[Client,Course,User,r]: zV[r,lb,Client,Course]
   + zV[r,rt,Client,User] + zV[r,lb,Course,User] <= 2
 triangle2[Client,Course,User,c]: zV[c,lb,Client,Course]
   + zV[c,rt,Client,User] + zV[c,lb,Course,User] <= 2
 triangle2[Client,Course,CourseSession,r]: zV[r,lb,Client,Course]
   + zV[r,rt,Client,CourseSession] + zV[r,lb,Course,CourseSession] <= 2
 triangle2[Client,Course,CourseSession,c]: zV[c,lb,Client,Course]
   + zV[c,rt,Client,CourseSession] + zV[c,lb,Course,CourseSession] <= 2
 triangle2[Client,Course,Clinic,r]: zV[r,lb,Client,Course]
   + zV[r,rt,Client,Clinic] + zV[r,lb,Course,Clinic] <= 2
 triangle2[Client,Course,Clinic,c]: zV[c,lb,Client,Course]
   + zV[c,rt,Client,Clinic] + zV[c,lb,Course,Clinic] <= 2
 triangle2[Client,Course,Location,r]: zV[r,lb,Client,Course]
   + zV[r,rt,Client,Location] + zV[r,lb,Course,Location] <= 2
 triangle2[Client,Course,Location,c]: zV[c,lb,Client,Course]
   + zV[c,rt,Client,Location] + zV[c,lb,Course,Location] <= 2
 triangle2[Client,Course,Therapist,r]: zV[r,lb,Client,Course]
   + zV[r,rt,Client,Therapist] + zV[r,lb,Course,Therapist] <= 2
 triangle2[Client,Course,Therapist,c]: zV[c,lb,Client,Course]
   + zV[c,rt,Client,Therapist] + zV[c,lb,Course,Therapist] <= 2
 triangle2[Client,Course,Instructor,r]: zV[r,lb,Client,Course]
   + zV[r,rt,Client,Instructor] + zV[r,lb,Course,Instructor] <= 2
 triangle2[Client,Course,Instructor,c]: zV[c,lb,Client,Course]
   + zV[c,rt,Client,Instructor] + zV[c,lb,Course,Instructor] <= 2
 triangle2[Client,Role,User,r]: zV[r,lb,Client,Role] + zV[r,rt,Client,User]
   + zV[r,lb,Role,User] <= 2
 triangle2[Client,Role,User,c]: zV[c,lb,Client,Role] + zV[c,rt,Client,User]
   + zV[c,lb,Role,User] <= 2
 triangle2[Client,Role,CourseSession,r]: zV[r,lb,Client,Role]
   + zV[r,rt,Client,CourseSession] + zV[r,lb,Role,CourseSession] <= 2
 triangle2[Client,Role,CourseSession,c]: zV[c,lb,Client,Role]
   + zV[c,rt,Client,CourseSession] + zV[c,lb,Role,CourseSession] <= 2
 triangle2[Client,Role,Clinic,r]: zV[r,lb,Client,Role]
   + zV[r,rt,Client,Clinic] + zV[r,lb,Role,Clinic] <= 2
 triangle2[Client,Role,Clinic,c]: zV[c,lb,Client,Role]
   + zV[c,rt,Client,Clinic] + zV[c,lb,Role,Clinic] <= 2
 triangle2[Client,Role,Location,r]: zV[r,lb,Client,Role]
   + zV[r,rt,Client,Location] + zV[r,lb,Role,Location] <= 2
 triangle2[Client,Role,Location,c]: zV[c,lb,Client,Role]
   + zV[c,rt,Client,Location] + zV[c,lb,Role,Location] <= 2
 triangle2[Client,Role,Therapist,r]: zV[r,lb,Client,Role]
   + zV[r,rt,Client,Therapist] + zV[r,lb,Role,Therapist] <= 2
 triangle2[Client,Role,Therapist,c]: zV[c,lb,Client,Role]
   + zV[c,rt,Client,Therapist] + zV[c,lb,Role,Therapist] <= 2
 triangle2[Client,Role,Instructor,r]: zV[r,lb,Client,Role]
   + zV[r,rt,Client,Instructor] + zV[r,lb,Role,Instructor] <= 2
 triangle2[Client,Role,Instructor,c]: zV[c,lb,Client,Role]
   + zV[c,rt,Client,Instructor] + zV[c,lb,Role,Instructor] <= 2
 triangle2[Client,User,CourseSession,r]: zV[r,lb,Client,User]
   + zV[r,rt,Client,CourseSession] + zV[r,lb,User,CourseSession] <= 2
 triangle2[Client,User,CourseSession,c]: zV[c,lb,Client,User]
   + zV[c,rt,Client,CourseSession] + zV[c,lb,User,CourseSession] <= 2
 triangle2[Client,User,Clinic,r]: zV[r,lb,Client,User]
   + zV[r,rt,Client,Clinic] + zV[r,lb,User,Clinic] <= 2
 triangle2[Client,User,Clinic,c]: zV[c,lb,Client,User]
   + zV[c,rt,Client,Clinic] + zV[c,lb,User,Clinic] <= 2
 triangle2[Client,User,Location,r]: zV[r,lb,Client,User]
   + zV[r,rt,Client,Location] + zV[r,lb,User,Location] <= 2
 triangle2[Client,User,Location,c]: zV[c,lb,Client,User]
   + zV[c,rt,Client,Location] + zV[c,lb,User,Location] <= 2
 triangle2[Client,User,Therapist,r]: zV[r,lb,Client,User]
   + zV[r,rt,Client,Therapist] + zV[r,lb,User,Therapist] <= 2
 triangle2[Client,User,Therapist,c]: zV[c,lb,Client,User]
   + zV[c,rt,Client,Therapist] + zV[c,lb,User,Therapist] <= 2
 triangle2[Client,User,Instructor,r]: zV[r,lb,Client,User]
   + zV[r,rt,Client,Instructor] + zV[r,lb,User,Instructor] <= 2
 triangle2[Client,User,Instructor,c]: zV[c,lb,Client,User]
   + zV[c,rt,Client,Instructor] + zV[c,lb,User,Instructor] <= 2
 triangle2[Client,CourseSession,Clinic,r]: zV[r,lb,Client,CourseSession]
   + zV[r,rt,Client,Clinic] + zV[r,lb,CourseSession,Clinic] <= 2
 triangle2[Client,CourseSession,Clinic,c]: zV[c,lb,Client,CourseSession]
   + zV[c,rt,Client,Clinic] + zV[c,lb,CourseSession,Clinic] <= 2
 triangle2[Client,CourseSession,Location,r]: zV[r,lb,Client,CourseSession]
   + zV[r,rt,Client,Location] + zV[r,lb,CourseSession,Location] <= 2
 triangle2[Client,CourseSession,Location,c]: zV[c,lb,Client,CourseSession]
   + zV[c,rt,Client,Location] + zV[c,lb,CourseSession,Location] <= 2
 triangle2[Client,CourseSession,Therapist,r]: zV[r,lb,Client,CourseSession]
   + zV[r,rt,Client,Therapist] + zV[r,lb,CourseSession,Therapist] <= 2
 triangle2[Client,CourseSession,Therapist,c]: zV[c,lb,Client,CourseSession]
   + zV[c,rt,Client,Therapist] + zV[c,lb,CourseSession,Therapist] <= 2
 triangle2[Client,CourseSession,Instructor,r]:
   zV[r,lb,Client,CourseSession] + zV[r,rt,Client,Instructor]
   + zV[r,lb,CourseSession,Instructor] <= 2
 triangle2[Client,CourseSession,Instructor,c]:
   zV[c,lb,Client,CourseSession] + zV[c,rt,Client,Instructor]
   + zV[c,lb,CourseSession,Instructor] <= 2
 triangle2[Client,Clinic,Location,r]: zV[r,lb,Client,Clinic]
   + zV[r,rt,Client,Location] + zV[r,lb,Clinic,Location] <= 2
 triangle2[Client,Clinic,Location,c]: zV[c,lb,Client,Clinic]
   + zV[c,rt,Client,Location] + zV[c,lb,Clinic,Location] <= 2
 triangle2[Client,Clinic,Therapist,r]: zV[r,lb,Client,Clinic]
   + zV[r,rt,Client,Therapist] + zV[r,lb,Clinic,Therapist] <= 2
 triangle2[Client,Clinic,Therapist,c]: zV[c,lb,Client,Clinic]
   + zV[c,rt,Client,Therapist] + zV[c,lb,Clinic,Therapist] <= 2
 triangle2[Client,Clinic,Instructor,r]: zV[r,lb,Client,Clinic]
   + zV[r,rt,Client,Instructor] + zV[r,lb,Clinic,Instructor] <= 2
 triangle2[Client,Clinic,Instructor,c]: zV[c,lb,Client,Clinic]
   + zV[c,rt,Client,Instructor] + zV[c,lb,Clinic,Instructor] <= 2
 triangle2[Client,Location,Therapist,r]: zV[r,lb,Client,Location]
   + zV[r,rt,Client,Therapist] + zV[r,lb,Location,Therapist] <= 2
 triangle2[Client,Location,Therapist,c]: zV[c,lb,Client,Location]
   + zV[c,rt,Client,Therapist] + zV[c,lb,Location,Therapist] <= 2
 triangle2[Client,Location,Instructor,r]: zV[r,lb,Client,Location]
   + zV[r,rt,Client,Instructor] + zV[r,lb,Location,Instructor] <= 2
 triangle2[Client,Location,Instructor,c]: zV[c,lb,Client,Location]
   + zV[c,rt,Client,Instructor] + zV[c,lb,Location,Instructor] <= 2
 triangle2[Client,Therapist,Instructor,r]: zV[r,lb,Client,Therapist]
   + zV[r,rt,Client,Instructor] + zV[r,lb,Therapist,Instructor] <= 2
 triangle2[Client,Therapist,Instructor,c]: zV[c,lb,Client,Therapist]
   + zV[c,rt,Client,Instructor] + zV[c,lb,Therapist,Instructor] <= 2
 triangle2[CertificateTemplate,Course,Role,r]:
   zV[r,lb,CertificateTemplate,Course] + zV[r,rt,CertificateTemplate,Role]
   + zV[r,lb,Course,Role] <= 2
 triangle2[CertificateTemplate,Course,Role,c]:
   zV[c,lb,CertificateTemplate,Course] + zV[c,rt,CertificateTemplate,Role]
   + zV[c,lb,Course,Role] <= 2
 triangle2[CertificateTemplate,Course,User,r]:
   zV[r,lb,CertificateTemplate,Course] + zV[r,rt,CertificateTemplate,User]
   + zV[r,lb,Course,User] <= 2
 triangle2[CertificateTemplate,Course,User,c]:
   zV[c,lb,CertificateTemplate,Course] + zV[c,rt,CertificateTemplate,User]
   + zV[c,lb,Course,User] <= 2
 triangle2[CertificateTemplate,Course,CourseSession,r]:
   zV[r,lb,CertificateTemplate,Course]
   + zV[r,rt,CertificateTemplate,CourseSession]
   + zV[r,lb,Course,CourseSession] <= 2
 triangle2[CertificateTemplate,Course,CourseSession,c]:
   zV[c,lb,CertificateTemplate,Course]
   + zV[c,rt,CertificateTemplate,CourseSession]
   + zV[c,lb,Course,CourseSession] <= 2
 triangle2[CertificateTemplate,Course,Clinic,r]:
   zV[r,lb,CertificateTemplate,Course]
   + zV[r,rt,CertificateTemplate,Clinic] + zV[r,lb,Course,Clinic] <= 2
 triangle2[CertificateTemplate,Course,Clinic,c]:
   zV[c,lb,CertificateTemplate,Course]
   + zV[c,rt,CertificateTemplate,Clinic] + zV[c,lb,Course,Clinic] <= 2
 triangle2[CertificateTemplate,Course,Location,r]:
   zV[r,lb,CertificateTemplate,Course]
   + zV[r,rt,CertificateTemplate,Location] + zV[r,lb,Course,Location] <= 2
 triangle2[CertificateTemplate,Course,Location,c]:
   zV[c,lb,CertificateTemplate,Course]
   + zV[c,rt,CertificateTemplate,Location] + zV[c,lb,Course,Location] <= 2
 triangle2[CertificateTemplate,Course,Therapist,r]:
   zV[r,lb,CertificateTemplate,Course]
   + zV[r,rt,CertificateTemplate,Therapist] + zV[r,lb,Course,Therapist]
   <= 2
 triangle2[CertificateTemplate,Course,Therapist,c]:
   zV[c,lb,CertificateTemplate,Course]
   + zV[c,rt,CertificateTemplate,Therapist] + zV[c,lb,Course,Therapist]
   <= 2
 triangle2[CertificateTemplate,Course,Instructor,r]:
   zV[r,lb,CertificateTemplate,Course]
   + zV[r,rt,CertificateTemplate,Instructor] + zV[r,lb,Course,Instructor]
   <= 2
 triangle2[CertificateTemplate,Course,Instructor,c]:
   zV[c,lb,CertificateTemplate,Course]
   + zV[c,rt,CertificateTemplate,Instructor] + zV[c,lb,Course,Instructor]
   <= 2
 triangle2[CertificateTemplate,Role,User,r]:
   zV[r,lb,CertificateTemplate,Role] + zV[r,rt,CertificateTemplate,User]
   + zV[r,lb,Role,User] <= 2
 triangle2[CertificateTemplate,Role,User,c]:
   zV[c,lb,CertificateTemplate,Role] + zV[c,rt,CertificateTemplate,User]
   + zV[c,lb,Role,User] <= 2
 triangle2[CertificateTemplate,Role,CourseSession,r]:
   zV[r,lb,CertificateTemplate,Role]
   + zV[r,rt,CertificateTemplate,CourseSession]
   + zV[r,lb,Role,CourseSession] <= 2
 triangle2[CertificateTemplate,Role,CourseSession,c]:
   zV[c,lb,CertificateTemplate,Role]
   + zV[c,rt,CertificateTemplate,CourseSession]
   + zV[c,lb,Role,CourseSession] <= 2
 triangle2[CertificateTemplate,Role,Clinic,r]:
   zV[r,lb,CertificateTemplate,Role] + zV[r,rt,CertificateTemplate,Clinic]
   + zV[r,lb,Role,Clinic] <= 2
 triangle2[CertificateTemplate,Role,Clinic,c]:
   zV[c,lb,CertificateTemplate,Role] + zV[c,rt,CertificateTemplate,Clinic]
   + zV[c,lb,Role,Clinic] <= 2
 triangle2[CertificateTemplate,Role,Location,r]:
   zV[r,lb,CertificateTemplate,Role]
   + zV[r,rt,CertificateTemplate,Location] + zV[r,lb,Role,Location] <= 2
 triangle2[CertificateTemplate,Role,Location,c]:
   zV[c,lb,CertificateTemplate,Role]
   + zV[c,rt,CertificateTemplate,Location] + zV[c,lb,Role,Location] <= 2
 triangle2[CertificateTemplate,Role,Therapist,r]:
   zV[r,lb,CertificateTemplate,Role]
   + zV[r,rt,CertificateTemplate,Therapist] + zV[r,lb,Role,Therapist] <= 2
 triangle2[CertificateTemplate,Role,Therapist,c]:
   zV[c,lb,CertificateTemplate,Role]
   + zV[c,rt,CertificateTemplate,Therapist] + zV[c,lb,Role,Therapist] <= 2
 triangle2[CertificateTemplate,Role,Instructor,r]:
   zV[r,lb,CertificateTemplate,Role]
   + zV[r,rt,CertificateTemplate,Instructor] + zV[r,lb,Role,Instructor]
   <= 2
 triangle2[CertificateTemplate,Role,Instructor,c]:
   zV[c,lb,CertificateTemplate,Role]
   + zV[c,rt,CertificateTemplate,Instructor] + zV[c,lb,Role,Instructor]
   <= 2
 triangle2[CertificateTemplate,User,CourseSession,r]:
   zV[r,lb,CertificateTemplate,User]
   + zV[r,rt,CertificateTemplate,CourseSession]
   + zV[r,lb,User,CourseSession] <= 2
 triangle2[CertificateTemplate,User,CourseSession,c]:
   zV[c,lb,CertificateTemplate,User]
   + zV[c,rt,CertificateTemplate,CourseSession]
   + zV[c,lb,User,CourseSession] <= 2
 triangle2[CertificateTemplate,User,Clinic,r]:
   zV[r,lb,CertificateTemplate,User] + zV[r,rt,CertificateTemplate,Clinic]
   + zV[r,lb,User,Clinic] <= 2
 triangle2[CertificateTemplate,User,Clinic,c]:
   zV[c,lb,CertificateTemplate,User] + zV[c,rt,CertificateTemplate,Clinic]
   + zV[c,lb,User,Clinic] <= 2
 triangle2[CertificateTemplate,User,Location,r]:
   zV[r,lb,CertificateTemplate,User]
   + zV[r,rt,CertificateTemplate,Location] + zV[r,lb,User,Location] <= 2
 triangle2[CertificateTemplate,User,Location,c]:
   zV[c,lb,CertificateTemplate,User]
   + zV[c,rt,CertificateTemplate,Location] + zV[c,lb,User,Location] <= 2
 triangle2[CertificateTemplate,User,Therapist,r]:
   zV[r,lb,CertificateTemplate,User]
   + zV[r,rt,CertificateTemplate,Therapist] + zV[r,lb,User,Therapist] <= 2
 triangle2[CertificateTemplate,User,Therapist,c]:
   zV[c,lb,CertificateTemplate,User]
   + zV[c,rt,CertificateTemplate,Therapist] + zV[c,lb,User,Therapist] <= 2
 triangle2[CertificateTemplate,User,Instructor,r]:
   zV[r,lb,CertificateTemplate,User]
   + zV[r,rt,CertificateTemplate,Instructor] + zV[r,lb,User,Instructor]
   <= 2
 triangle2[CertificateTemplate,User,Instructor,c]:
   zV[c,lb,CertificateTemplate,User]
   + zV[c,rt,CertificateTemplate,Instructor] + zV[c,lb,User,Instructor]
   <= 2
 triangle2[CertificateTemplate,CourseSession,Clinic,r]:
   zV[r,lb,CertificateTemplate,CourseSession]
   + zV[r,rt,CertificateTemplate,Clinic] + zV[r,lb,CourseSession,Clinic]
   <= 2
 triangle2[CertificateTemplate,CourseSession,Clinic,c]:
   zV[c,lb,CertificateTemplate,CourseSession]
   + zV[c,rt,CertificateTemplate,Clinic] + zV[c,lb,CourseSession,Clinic]
   <= 2
 triangle2[CertificateTemplate,CourseSession,Location,r]:
   zV[r,lb,CertificateTemplate,CourseSession]
   + zV[r,rt,CertificateTemplate,Location]
   + zV[r,lb,CourseSession,Location] <= 2
 triangle2[CertificateTemplate,CourseSession,Location,c]:
   zV[c,lb,CertificateTemplate,CourseSession]
   + zV[c,rt,CertificateTemplate,Location]
   + zV[c,lb,CourseSession,Location] <= 2
 triangle2[CertificateTemplate,CourseSession,Therapist,r]:
   zV[r,lb,CertificateTemplate,CourseSession]
   + zV[r,rt,CertificateTemplate,Therapist]
   + zV[r,lb,CourseSession,Therapist] <= 2
 triangle2[CertificateTemplate,CourseSession,Therapist,c]:
   zV[c,lb,CertificateTemplate,CourseSession]
   + zV[c,rt,CertificateTemplate,Therapist]
   + zV[c,lb,CourseSession,Therapist] <= 2
 triangle2[CertificateTemplate,CourseSession,Instructor,r]:
   zV[r,lb,CertificateTemplate,CourseSession]
   + zV[r,rt,CertificateTemplate,Instructor]
   + zV[r,lb,CourseSession,Instructor] <= 2
 triangle2[CertificateTemplate,CourseSession,Instructor,c]:
   zV[c,lb,CertificateTemplate,CourseSession]
   + zV[c,rt,CertificateTemplate,Instructor]
   + zV[c,lb,CourseSession,Instructor] <= 2
 triangle2[CertificateTemplate,Clinic,Location,r]:
   zV[r,lb,CertificateTemplate,Clinic]
   + zV[r,rt,CertificateTemplate,Location] + zV[r,lb,Clinic,Location] <= 2
 triangle2[CertificateTemplate,Clinic,Location,c]:
   zV[c,lb,CertificateTemplate,Clinic]
   + zV[c,rt,CertificateTemplate,Location] + zV[c,lb,Clinic,Location] <= 2
 triangle2[CertificateTemplate,Clinic,Therapist,r]:
   zV[r,lb,CertificateTemplate,Clinic]
   + zV[r,rt,CertificateTemplate,Therapist] + zV[r,lb,Clinic,Therapist]
   <= 2
 triangle2[CertificateTemplate,Clinic,Therapist,c]:
   zV[c,lb,CertificateTemplate,Clinic]
   + zV[c,rt,CertificateTemplate,Therapist] + zV[c,lb,Clinic,Therapist]
   <= 2
 triangle2[CertificateTemplate,Clinic,Instructor,r]:
   zV[r,lb,CertificateTemplate,Clinic]
   + zV[r,rt,CertificateTemplate,Instructor] + zV[r,lb,Clinic,Instructor]
   <= 2
 triangle2[CertificateTemplate,Clinic,Instructor,c]:
   zV[c,lb,CertificateTemplate,Clinic]
   + zV[c,rt,CertificateTemplate,Instructor] + zV[c,lb,Clinic,Instructor]
   <= 2
 triangle2[CertificateTemplate,Location,Therapist,r]:
   zV[r,lb,CertificateTemplate,Location]
   + zV[r,rt,CertificateTemplate,Therapist] + zV[r,lb,Location,Therapist]
   <= 2
 triangle2[CertificateTemplate,Location,Therapist,c]:
   zV[c,lb,CertificateTemplate,Location]
   + zV[c,rt,CertificateTemplate,Therapist] + zV[c,lb,Location,Therapist]
   <= 2
 triangle2[CertificateTemplate,Location,Instructor,r]:
   zV[r,lb,CertificateTemplate,Location]
   + zV[r,rt,CertificateTemplate,Instructor] + zV[r,lb,Location,Instructor]
   <= 2
 triangle2[CertificateTemplate,Location,Instructor,c]:
   zV[c,lb,CertificateTemplate,Location]
   + zV[c,rt,CertificateTemplate,Instructor] + zV[c,lb,Location,Instructor]
   <= 2
 triangle2[CertificateTemplate,Therapist,Instructor,r]:
   zV[r,lb,CertificateTemplate,Therapist]
   + zV[r,rt,CertificateTemplate,Instructor]
   + zV[r,lb,Therapist,Instructor] <= 2
 triangle2[CertificateTemplate,Therapist,Instructor,c]:
   zV[c,lb,CertificateTemplate,Therapist]
   + zV[c,rt,CertificateTemplate,Instructor]
   + zV[c,lb,Therapist,Instructor] <= 2
 triangle2[Course,Role,User,r]: zV[r,lb,Course,Role] + zV[r,rt,Course,User]
   + zV[r,lb,Role,User] <= 2
 triangle2[Course,Role,User,c]: zV[c,lb,Course,Role] + zV[c,rt,Course,User]
   + zV[c,lb,Role,User] <= 2
 triangle2[Course,Role,CourseSession,r]: zV[r,lb,Course,Role]
   + zV[r,rt,Course,CourseSession] + zV[r,lb,Role,CourseSession] <= 2
 triangle2[Course,Role,CourseSession,c]: zV[c,lb,Course,Role]
   + zV[c,rt,Course,CourseSession] + zV[c,lb,Role,CourseSession] <= 2
 triangle2[Course,Role,Clinic,r]: zV[r,lb,Course,Role]
   + zV[r,rt,Course,Clinic] + zV[r,lb,Role,Clinic] <= 2
 triangle2[Course,Role,Clinic,c]: zV[c,lb,Course,Role]
   + zV[c,rt,Course,Clinic] + zV[c,lb,Role,Clinic] <= 2
 triangle2[Course,Role,Location,r]: zV[r,lb,Course,Role]
   + zV[r,rt,Course,Location] + zV[r,lb,Role,Location] <= 2
 triangle2[Course,Role,Location,c]: zV[c,lb,Course,Role]
   + zV[c,rt,Course,Location] + zV[c,lb,Role,Location] <= 2
 triangle2[Course,Role,Therapist,r]: zV[r,lb,Course,Role]
   + zV[r,rt,Course,Therapist] + zV[r,lb,Role,Therapist] <= 2
 triangle2[Course,Role,Therapist,c]: zV[c,lb,Course,Role]
   + zV[c,rt,Course,Therapist] + zV[c,lb,Role,Therapist] <= 2
 triangle2[Course,Role,Instructor,r]: zV[r,lb,Course,Role]
   + zV[r,rt,Course,Instructor] + zV[r,lb,Role,Instructor] <= 2
 triangle2[Course,Role,Instructor,c]: zV[c,lb,Course,Role]
   + zV[c,rt,Course,Instructor] + zV[c,lb,Role,Instructor] <= 2
 triangle2[Course,User,CourseSession,r]: zV[r,lb,Course,User]
   + zV[r,rt,Course,CourseSession] + zV[r,lb,User,CourseSession] <= 2
 triangle2[Course,User,CourseSession,c]: zV[c,lb,Course,User]
   + zV[c,rt,Course,CourseSession] + zV[c,lb,User,CourseSession] <= 2
 triangle2[Course,User,Clinic,r]: zV[r,lb,Course,User]
   + zV[r,rt,Course,Clinic] + zV[r,lb,User,Clinic] <= 2
 triangle2[Course,User,Clinic,c]: zV[c,lb,Course,User]
   + zV[c,rt,Course,Clinic] + zV[c,lb,User,Clinic] <= 2
 triangle2[Course,User,Location,r]: zV[r,lb,Course,User]
   + zV[r,rt,Course,Location] + zV[r,lb,User,Location] <= 2
 triangle2[Course,User,Location,c]: zV[c,lb,Course,User]
   + zV[c,rt,Course,Location] + zV[c,lb,User,Location] <= 2
 triangle2[Course,User,Therapist,r]: zV[r,lb,Course,User]
   + zV[r,rt,Course,Therapist] + zV[r,lb,User,Therapist] <= 2
 triangle2[Course,User,Therapist,c]: zV[c,lb,Course,User]
   + zV[c,rt,Course,Therapist] + zV[c,lb,User,Therapist] <= 2
 triangle2[Course,User,Instructor,r]: zV[r,lb,Course,User]
   + zV[r,rt,Course,Instructor] + zV[r,lb,User,Instructor] <= 2
 triangle2[Course,User,Instructor,c]: zV[c,lb,Course,User]
   + zV[c,rt,Course,Instructor] + zV[c,lb,User,Instructor] <= 2
 triangle2[Course,CourseSession,Clinic,r]: zV[r,lb,Course,CourseSession]
   + zV[r,rt,Course,Clinic] + zV[r,lb,CourseSession,Clinic] <= 2
 triangle2[Course,CourseSession,Clinic,c]: zV[c,lb,Course,CourseSession]
   + zV[c,rt,Course,Clinic] + zV[c,lb,CourseSession,Clinic] <= 2
 triangle2[Course,CourseSession,Location,r]: zV[r,lb,Course,CourseSession]
   + zV[r,rt,Course,Location] + zV[r,lb,CourseSession,Location] <= 2
 triangle2[Course,CourseSession,Location,c]: zV[c,lb,Course,CourseSession]
   + zV[c,rt,Course,Location] + zV[c,lb,CourseSession,Location] <= 2
 triangle2[Course,CourseSession,Therapist,r]: zV[r,lb,Course,CourseSession]
   + zV[r,rt,Course,Therapist] + zV[r,lb,CourseSession,Therapist] <= 2
 triangle2[Course,CourseSession,Therapist,c]: zV[c,lb,Course,CourseSession]
   + zV[c,rt,Course,Therapist] + zV[c,lb,CourseSession,Therapist] <= 2
 triangle2[Course,CourseSession,Instructor,r]:
   zV[r,lb,Course,CourseSession] + zV[r,rt,Course,Instructor]
   + zV[r,lb,CourseSession,Instructor] <= 2
 triangle2[Course,CourseSession,Instructor,c]:
   zV[c,lb,Course,CourseSession] + zV[c,rt,Course,Instructor]
   + zV[c,lb,CourseSession,Instructor] <= 2
 triangle2[Course,Clinic,Location,r]: zV[r,lb,Course,Clinic]
   + zV[r,rt,Course,Location] + zV[r,lb,Clinic,Location] <= 2
 triangle2[Course,Clinic,Location,c]: zV[c,lb,Course,Clinic]
   + zV[c,rt,Course,Location] + zV[c,lb,Clinic,Location] <= 2
 triangle2[Course,Clinic,Therapist,r]: zV[r,lb,Course,Clinic]
   + zV[r,rt,Course,Therapist] + zV[r,lb,Clinic,Therapist] <= 2
 triangle2[Course,Clinic,Therapist,c]: zV[c,lb,Course,Clinic]
   + zV[c,rt,Course,Therapist] + zV[c,lb,Clinic,Therapist] <= 2
 triangle2[Course,Clinic,Instructor,r]: zV[r,lb,Course,Clinic]
   + zV[r,rt,Course,Instructor] + zV[r,lb,Clinic,Instructor] <= 2
 triangle2[Course,Clinic,Instructor,c]: zV[c,lb,Course,Clinic]
   + zV[c,rt,Course,Instructor] + zV[c,lb,Clinic,Instructor] <= 2
 triangle2[Course,Location,Therapist,r]: zV[r,lb,Course,Location]
   + zV[r,rt,Course,Therapist] + zV[r,lb,Location,Therapist] <= 2
 triangle2[Course,Location,Therapist,c]: zV[c,lb,Course,Location]
   + zV[c,rt,Course,Therapist] + zV[c,lb,Location,Therapist] <= 2
 triangle2[Course,Location,Instructor,r]: zV[r,lb,Course,Location]
   + zV[r,rt,Course,Instructor] + zV[r,lb,Location,Instructor] <= 2
 triangle2[Course,Location,Instructor,c]: zV[c,lb,Course,Location]
   + zV[c,rt,Course,Instructor] + zV[c,lb,Location,Instructor] <= 2
 triangle2[Course,Therapist,Instructor,r]: zV[r,lb,Course,Therapist]
   + zV[r,rt,Course,Instructor] + zV[r,lb,Therapist,Instructor] <= 2
 triangle2[Course,Therapist,Instructor,c]: zV[c,lb,Course,Therapist]
   + zV[c,rt,Course,Instructor] + zV[c,lb,Therapist,Instructor] <= 2
 triangle2[Role,User,CourseSession,r]: zV[r,lb,Role,User]
   + zV[r,rt,Role,CourseSession] + zV[r,lb,User,CourseSession] <= 2
 triangle2[Role,User,CourseSession,c]: zV[c,lb,Role,User]
   + zV[c,rt,Role,CourseSession] + zV[c,lb,User,CourseSession] <= 2
 triangle2[Role,User,Clinic,r]: zV[r,lb,Role,User] + zV[r,rt,Role,Clinic]
   + zV[r,lb,User,Clinic] <= 2
 triangle2[Role,User,Clinic,c]: zV[c,lb,Role,User] + zV[c,rt,Role,Clinic]
   + zV[c,lb,User,Clinic] <= 2
 triangle2[Role,User,Location,r]: zV[r,lb,Role,User]
   + zV[r,rt,Role,Location] + zV[r,lb,User,Location] <= 2
 triangle2[Role,User,Location,c]: zV[c,lb,Role,User]
   + zV[c,rt,Role,Location] + zV[c,lb,User,Location] <= 2
 triangle2[Role,User,Therapist,r]: zV[r,lb,Role,User]
   + zV[r,rt,Role,Therapist] + zV[r,lb,User,Therapist] <= 2
 triangle2[Role,User,Therapist,c]: zV[c,lb,Role,User]
   + zV[c,rt,Role,Therapist] + zV[c,lb,User,Therapist] <= 2
 triangle2[Role,User,Instructor,r]: zV[r,lb,Role,User]
   + zV[r,rt,Role,Instructor] + zV[r,lb,User,Instructor] <= 2
 triangle2[Role,User,Instructor,c]: zV[c,lb,Role,User]
   + zV[c,rt,Role,Instructor] + zV[c,lb,User,Instructor] <= 2
 triangle2[Role,CourseSession,Clinic,r]: zV[r,lb,Role,CourseSession]
   + zV[r,rt,Role,Clinic] + zV[r,lb,CourseSession,Clinic] <= 2
 triangle2[Role,CourseSession,Clinic,c]: zV[c,lb,Role,CourseSession]
   + zV[c,rt,Role,Clinic] + zV[c,lb,CourseSession,Clinic] <= 2
 triangle2[Role,CourseSession,Location,r]: zV[r,lb,Role,CourseSession]
   + zV[r,rt,Role,Location] + zV[r,lb,CourseSession,Location] <= 2
 triangle2[Role,CourseSession,Location,c]: zV[c,lb,Role,CourseSession]
   + zV[c,rt,Role,Location] + zV[c,lb,CourseSession,Location] <= 2
 triangle2[Role,CourseSession,Therapist,r]: zV[r,lb,Role,CourseSession]
   + zV[r,rt,Role,Therapist] + zV[r,lb,CourseSession,Therapist] <= 2
 triangle2[Role,CourseSession,Therapist,c]: zV[c,lb,Role,CourseSession]
   + zV[c,rt,Role,Therapist] + zV[c,lb,CourseSession,Therapist] <= 2
 triangle2[Role,CourseSession,Instructor,r]: zV[r,lb,Role,CourseSession]
   + zV[r,rt,Role,Instructor] + zV[r,lb,CourseSession,Instructor] <= 2
 triangle2[Role,CourseSession,Instructor,c]: zV[c,lb,Role,CourseSession]
   + zV[c,rt,Role,Instructor] + zV[c,lb,CourseSession,Instructor] <= 2
 triangle2[Role,Clinic,Location,r]: zV[r,lb,Role,Clinic]
   + zV[r,rt,Role,Location] + zV[r,lb,Clinic,Location] <= 2
 triangle2[Role,Clinic,Location,c]: zV[c,lb,Role,Clinic]
   + zV[c,rt,Role,Location] + zV[c,lb,Clinic,Location] <= 2
 triangle2[Role,Clinic,Therapist,r]: zV[r,lb,Role,Clinic]
   + zV[r,rt,Role,Therapist] + zV[r,lb,Clinic,Therapist] <= 2
 triangle2[Role,Clinic,Therapist,c]: zV[c,lb,Role,Clinic]
   + zV[c,rt,Role,Therapist] + zV[c,lb,Clinic,Therapist] <= 2
 triangle2[Role,Clinic,Instructor,r]: zV[r,lb,Role,Clinic]
   + zV[r,rt,Role,Instructor] + zV[r,lb,Clinic,Instructor] <= 2
 triangle2[Role,Clinic,Instructor,c]: zV[c,lb,Role,Clinic]
   + zV[c,rt,Role,Instructor] + zV[c,lb,Clinic,Instructor] <= 2
 triangle2[Role,Location,Therapist,r]: zV[r,lb,Role,Location]
   + zV[r,rt,Role,Therapist] + zV[r,lb,Location,Therapist] <= 2
 triangle2[Role,Location,Therapist,c]: zV[c,lb,Role,Location]
   + zV[c,rt,Role,Therapist] + zV[c,lb,Location,Therapist] <= 2
 triangle2[Role,Location,Instructor,r]: zV[r,lb,Role,Location]
   + zV[r,rt,Role,Instructor] + zV[r,lb,Location,Instructor] <= 2
 triangle2[Role,Location,Instructor,c]: zV[c,lb,Role,Location]
   + zV[c,rt,Role,Instructor] + zV[c,lb,Location,Instructor] <= 2
 triangle2[Role,Therapist,Instructor,r]: zV[r,lb,Role,Therapist]
   + zV[r,rt,Role,Instructor] + zV[r,lb,Therapist,Instructor] <= 2
 triangle2[Role,Therapist,Instructor,c]: zV[c,lb,Role,Therapist]
   + zV[c,rt,Role,Instructor] + zV[c,lb,Therapist,Instructor] <= 2
 triangle2[User,CourseSession,Clinic,r]: zV[r,lb,User,CourseSession]
   + zV[r,rt,User,Clinic] + zV[r,lb,CourseSession,Clinic] <= 2
 triangle2[User,CourseSession,Clinic,c]: zV[c,lb,User,CourseSession]
   + zV[c,rt,User,Clinic] + zV[c,lb,CourseSession,Clinic] <= 2
 triangle2[User,CourseSession,Location,r]: zV[r,lb,User,CourseSession]
   + zV[r,rt,User,Location] + zV[r,lb,CourseSession,Location] <= 2
 triangle2[User,CourseSession,Location,c]: zV[c,lb,User,CourseSession]
   + zV[c,rt,User,Location] + zV[c,lb,CourseSession,Location] <= 2
 triangle2[User,CourseSession,Therapist,r]: zV[r,lb,User,CourseSession]
   + zV[r,rt,User,Therapist] + zV[r,lb,CourseSession,Therapist] <= 2
 triangle2[User,CourseSession,Therapist,c]: zV[c,lb,User,CourseSession]
   + zV[c,rt,User,Therapist] + zV[c,lb,CourseSession,Therapist] <= 2
 triangle2[User,CourseSession,Instructor,r]: zV[r,lb,User,CourseSession]
   + zV[r,rt,User,Instructor] + zV[r,lb,CourseSession,Instructor] <= 2
 triangle2[User,CourseSession,Instructor,c]: zV[c,lb,User,CourseSession]
   + zV[c,rt,User,Instructor] + zV[c,lb,CourseSession,Instructor] <= 2
 triangle2[User,Clinic,Location,r]: zV[r,lb,User,Clinic]
   + zV[r,rt,User,Location] + zV[r,lb,Clinic,Location] <= 2
 triangle2[User,Clinic,Location,c]: zV[c,lb,User,Clinic]
   + zV[c,rt,User,Location] + zV[c,lb,Clinic,Location] <= 2
 triangle2[User,Clinic,Therapist,r]: zV[r,lb,User,Clinic]
   + zV[r,rt,User,Therapist] + zV[r,lb,Clinic,Therapist] <= 2
 triangle2[User,Clinic,Therapist,c]: zV[c,lb,User,Clinic]
   + zV[c,rt,User,Therapist] + zV[c,lb,Clinic,Therapist] <= 2
 triangle2[User,Clinic,Instructor,r]: zV[r,lb,User,Clinic]
   + zV[r,rt,User,Instructor] + zV[r,lb,Clinic,Instructor] <= 2
 triangle2[User,Clinic,Instructor,c]: zV[c,lb,User,Clinic]
   + zV[c,rt,User,Instructor] + zV[c,lb,Clinic,Instructor] <= 2
 triangle2[User,Location,Therapist,r]: zV[r,lb,User,Location]
   + zV[r,rt,User,Therapist] + zV[r,lb,Location,Therapist] <= 2
 triangle2[User,Location,Therapist,c]: zV[c,lb,User,Location]
   + zV[c,rt,User,Therapist] + zV[c,lb,Location,Therapist] <= 2
 triangle2[User,Location,Instructor,r]: zV[r,lb,User,Location]
   + zV[r,rt,User,Instructor] + zV[r,lb,Location,Instructor] <= 2
 triangle2[User,Location,Instructor,c]: zV[c,lb,User,Location]
   + zV[c,rt,User,Instructor] + zV[c,lb,Location,Instructor] <= 2
 triangle2[User,Therapist,Instructor,r]: zV[r,lb,User,Therapist]
   + zV[r,rt,User,Instructor] + zV[r,lb,Therapist,Instructor] <= 2
 triangle2[User,Therapist,Instructor,c]: zV[c,lb,User,Therapist]
   + zV[c,rt,User,Instructor] + zV[c,lb,Therapist,Instructor] <= 2
 triangle2[CourseSession,Clinic,Location,r]: zV[r,lb,CourseSession,Clinic]
   + zV[r,rt,CourseSession,Location] + zV[r,lb,Clinic,Location] <= 2
 triangle2[CourseSession,Clinic,Location,c]: zV[c,lb,CourseSession,Clinic]
   + zV[c,rt,CourseSession,Location] + zV[c,lb,Clinic,Location] <= 2
 triangle2[CourseSession,Clinic,Therapist,r]: zV[r,lb,CourseSession,Clinic]
   + zV[r,rt,CourseSession,Therapist] + zV[r,lb,Clinic,Therapist] <= 2
 triangle2[CourseSession,Clinic,Therapist,c]: zV[c,lb,CourseSession,Clinic]
   + zV[c,rt,CourseSession,Therapist] + zV[c,lb,Clinic,Therapist] <= 2
 triangle2[CourseSession,Clinic,Instructor,r]:
   zV[r,lb,CourseSession,Clinic] + zV[r,rt,CourseSession,Instructor]
   + zV[r,lb,Clinic,Instructor] <= 2
 triangle2[CourseSession,Clinic,Instructor,c]:
   zV[c,lb,CourseSession,Clinic] + zV[c,rt,CourseSession,Instructor]
   + zV[c,lb,Clinic,Instructor] <= 2
 triangle2[CourseSession,Location,Therapist,r]:
   zV[r,lb,CourseSession,Location] + zV[r,rt,CourseSession,Therapist]
   + zV[r,lb,Location,Therapist] <= 2
 triangle2[CourseSession,Location,Therapist,c]:
   zV[c,lb,CourseSession,Location] + zV[c,rt,CourseSession,Therapist]
   + zV[c,lb,Location,Therapist] <= 2
 triangle2[CourseSession,Location,Instructor,r]:
   zV[r,lb,CourseSession,Location] + zV[r,rt,CourseSession,Instructor]
   + zV[r,lb,Location,Instructor] <= 2
 triangle2[CourseSession,Location,Instructor,c]:
   zV[c,lb,CourseSession,Location] + zV[c,rt,CourseSession,Instructor]
   + zV[c,lb,Location,Instructor] <= 2
 triangle2[CourseSession,Therapist,Instructor,r]:
   zV[r,lb,CourseSession,Therapist] + zV[r,rt,CourseSession,Instructor]
   + zV[r,lb,Therapist,Instructor] <= 2
 triangle2[CourseSession,Therapist,Instructor,c]:
   zV[c,lb,CourseSession,Therapist] + zV[c,rt,CourseSession,Instructor]
   + zV[c,lb,Therapist,Instructor] <= 2
 triangle2[Clinic,Location,Therapist,r]: zV[r,lb,Clinic,Location]
   + zV[r,rt,Clinic,Therapist] + zV[r,lb,Location,Therapist] <= 2
 triangle2[Clinic,Location,Therapist,c]: zV[c,lb,Clinic,Location]
   + zV[c,rt,Clinic,Therapist] + zV[c,lb,Location,Therapist] <= 2
 triangle2[Clinic,Location,Instructor,r]: zV[r,lb,Clinic,Location]
   + zV[r,rt,Clinic,Instructor] + zV[r,lb,Location,Instructor] <= 2
 triangle2[Clinic,Location,Instructor,c]: zV[c,lb,Clinic,Location]
   + zV[c,rt,Clinic,Instructor] + zV[c,lb,Location,Instructor] <= 2
 triangle2[Clinic,Therapist,Instructor,r]: zV[r,lb,Clinic,Therapist]
   + zV[r,rt,Clinic,Instructor] + zV[r,lb,Therapist,Instructor] <= 2
 triangle2[Clinic,Therapist,Instructor,c]: zV[c,lb,Clinic,Therapist]
   + zV[c,rt,Clinic,Instructor] + zV[c,lb,Therapist,Instructor] <= 2
 triangle2[Location,Therapist,Instructor,r]: zV[r,lb,Location,Therapist]
   + zV[r,rt,Location,Instructor] + zV[r,lb,Therapist,Instructor] <= 2
 triangle2[Location,Therapist,Instructor,c]: zV[c,lb,Location,Therapist]
   + zV[c,rt,Location,Instructor] + zV[c,lb,Therapist,Instructor] <= 2
Bounds
 1 <= x[r,Client] <= 34
 1 <= x[r,CertificateTemplate] <= 36
 1 <= x[r,Course] <= 37
 1 <= x[r,Role] <= 37
 1 <= x[r,User] <= 36
 1 <= x[r,CourseSession] <= 34
 1 <= x[r,Clinic] <= 35
 1 <= x[r,Location] <= 34
 1 <= x[r,Therapist] <= 33
 1 <= x[r,Instructor] <= 33
 1 <= x[c,Client] <= 32
 1 <= x[c,CertificateTemplate] <= 31
 1 <= x[c,Course] <= 33
 1 <= x[c,Role] <= 33
 1 <= x[c,User] <= 32
 1 <= x[c,CourseSession] <= 29
 1 <= x[c,Clinic] <= 32
 1 <= x[c,Location] <= 32
 1 <= x[c,Therapist] <= 32
 1 <= x[c,Instructor] <= 31
 center_dist[r,Client] free
 center_dist[r,CertificateTemplate] free
 center_dist[r,Course] free
 center_dist[r,Role] free
 center_dist[r,User] free
 center_dist[r,CourseSession] free
 center_dist[r,Clinic] free
 center_dist[r,Location] free
 center_dist[r,Therapist] free
 center_dist[r,Instructor] free
 center_dist[c,Client] free
 center_dist[c,CertificateTemplate] free
 center_dist[c,Course] free
 center_dist[c,Role] free
 center_dist[c,User] free
 center_dist[c,CourseSession] free
 center_dist[c,Clinic] free
 center_dist[c,Location] free
 center_dist[c,Therapist] free
 center_dist[c,Instructor] free
 dist[r,Client,CertificateTemplate] <= 43
 dist[r,Client,CourseSession] <= 42
 dist[r,Client,Instructor] <= 42.5
 dist[r,Client,Location] <= 42
 dist[r,Client,User] <= 43
 dist[r,CertificateTemplate,CourseSession] <= 43
 dist[r,Course,CourseSession] <= 43.5
 dist[r,CourseSession,Instructor] <= 42.5
 dist[r,Instructor,Location] <= 42.5
 dist[r,Clinic,Location] <= 42.5
 dist[r,Clinic,Therapist] <= 43
 dist[r,Location,Therapist] <= 42.5
 dist[r,Location,CourseSession] <= 42
 dist[r,Therapist,CourseSession] <= 42.5
 dist[r,Role,User] <= 42.5
 dist[r,User,Instructor] <= 43.5
 dist[r,User,Therapist] <= 43.5
 dist[c,Client,CertificateTemplate] <= 42.5
 dist[c,Client,CourseSession] <= 43.5
 dist[c,Client,Instructor] <= 42.5
 dist[c,Client,Location] <= 42
 dist[c,Client,User] <= 42
 dist[c,CertificateTemplate,CourseSession] <= 43
 dist[c,Course,CourseSession] <= 44
 dist[c,CourseSession,Instructor] <= 43
 dist[c,Instructor,Location] <= 42.5
 dist[c,Clinic,Location] <= 42
 dist[c,Clinic,Therapist] <= 42
 dist[c,Location,Therapist] <= 42
 dist[c,Location,CourseSession] <= 43.5
 dist[c,Therapist,CourseSession] <= 43.5
 dist[c,Role,User] <= 42.5
 dist[c,User,Instructor] <= 42.5
 dist[c,User,Therapist] <= 42
 zV[r,rt,Client,CertificateTemplate] <= 1
 zV[c,rt,Client,CertificateTemplate] <= 1
 zV[r,lb,Client,CertificateTemplate] <= 1
 zV[c,lb,Client,CertificateTemplate] <= 1
 zV[r,rt,Client,Course] <= 1
 zV[c,rt,Client,Course] <= 1
 zV[r,lb,Client,Course] <= 1
 zV[c,lb,Client,Course] <= 1
 zV[r,rt,Client,Role] <= 1
 zV[c,rt,Client,Role] <= 1
 zV[r,lb,Client,Role] <= 1
 zV[c,lb,Client,Role] <= 1
 zV[r,rt,Client,User] <= 1
 zV[c,rt,Client,User] <= 1
 zV[r,lb,Client,User] <= 1
 zV[c,lb,Client,User] <= 1
 zV[r,rt,Client,CourseSession] <= 1
 zV[c,rt,Client,CourseSession] <= 1
 zV[r,lb,Client,CourseSession] <= 1
 zV[c,lb,Client,CourseSession] <= 1
 zV[r,rt,Client,Clinic] <= 1
 zV[c,rt,Client,Clinic] <= 1
 zV[r,lb,Client,Clinic] <= 1
 zV[c,lb,Client,Clinic] <= 1
 zV[r,rt,Client,Location] <= 1
 zV[c,rt,Client,Location] <= 1
 zV[r,lb,Client,Location] <= 1
 zV[c,lb,Client,Location] <= 1
 zV[r,rt,Client,Therapist] <= 1
 zV[c,rt,Client,Therapist] <= 1
 zV[r,lb,Client,Therapist] <= 1
 zV[c,lb,Client,Therapist] <= 1
 zV[r,rt,Client,Instructor] <= 1
 zV[c,rt,Client,Instructor] <= 1
 zV[r,lb,Client,Instructor] <= 1
 zV[c,lb,Client,Instructor] <= 1
 zV[r,rt,CertificateTemplate,Course] <= 1
 zV[c,rt,CertificateTemplate,Course] <= 1
 zV[r,lb,CertificateTemplate,Course] <= 1
 zV[c,lb,CertificateTemplate,Course] <= 1
 zV[r,rt,CertificateTemplate,Role] <= 1
 zV[c,rt,CertificateTemplate,Role] <= 1
 zV[r,lb,CertificateTemplate,Role] <= 1
 zV[c,lb,CertificateTemplate,Role] <= 1
 zV[r,rt,CertificateTemplate,User] <= 1
 zV[c,rt,CertificateTemplate,User] <= 1
 zV[r,lb,CertificateTemplate,User] <= 1
 zV[c,lb,CertificateTemplate,User] <= 1
 zV[r,rt,CertificateTemplate,CourseSession] <= 1
 zV[c,rt,CertificateTemplate,CourseSession] <= 1
 zV[r,lb,CertificateTemplate,CourseSession] <= 1
 zV[c,lb,CertificateTemplate,CourseSession] <= 1
 zV[r,rt,CertificateTemplate,Clinic] <= 1
 zV[c,rt,CertificateTemplate,Clinic] <= 1
 zV[r,lb,CertificateTemplate,Clinic] <= 1
 zV[c,lb,CertificateTemplate,Clinic] <= 1
 zV[r,rt,CertificateTemplate,Location] <= 1
 zV[c,rt,CertificateTemplate,Location] <= 1
 zV[r,lb,CertificateTemplate,Location] <= 1
 zV[c,lb,CertificateTemplate,Location] <= 1
 zV[r,rt,CertificateTemplate,Therapist] <= 1
 zV[c,rt,CertificateTemplate,Therapist] <= 1
 zV[r,lb,CertificateTemplate,Therapist] <= 1
 zV[c,lb,CertificateTemplate,Therapist] <= 1
 zV[r,rt,CertificateTemplate,Instructor] <= 1
 zV[c,rt,CertificateTemplate,Instructor] <= 1
 zV[r,lb,CertificateTemplate,Instructor] <= 1
 zV[c,lb,CertificateTemplate,Instructor] <= 1
 zV[r,rt,Course,Role] <= 1
 zV[c,rt,Course,Role] <= 1
 zV[r,lb,Course,Role] <= 1
 zV[c,lb,Course,Role] <= 1
 zV[r,rt,Course,User] <= 1
 zV[c,rt,Course,User] <= 1
 zV[r,lb,Course,User] <= 1
 zV[c,lb,Course,User] <= 1
 zV[r,rt,Course,CourseSession] <= 1
 zV[c,rt,Course,CourseSession] <= 1
 zV[r,lb,Course,CourseSession] <= 1
 zV[c,lb,Course,CourseSession] <= 1
 zV[r,rt,Course,Clinic] <= 1
 zV[c,rt,Course,Clinic] <= 1
 zV[r,lb,Course,Clinic] <= 1
 zV[c,lb,Course,Clinic] <= 1
 zV[r,rt,Course,Location] <= 1
 zV[c,rt,Course,Location] <= 1
 zV[r,lb,Course,Location] <= 1
 zV[c,lb,Course,Location] <= 1
 zV[r,rt,Course,Therapist] <= 1
 zV[c,rt,Course,Therapist] <= 1
 zV[r,lb,Course,Therapist] <= 1
 zV[c,lb,Course,Therapist] <= 1
 zV[r,rt,Course,Instructor] <= 1
 zV[c,rt,Course,Instructor] <= 1
 zV[r,lb,Course,Instructor] <= 1
 zV[c,lb,Course,Instructor] <= 1
 zV[r,rt,Role,User] <= 1
 zV[c,rt,Role,User] <= 1
 zV[r,lb,Role,User] <= 1
 zV[c,lb,Role,User] <= 1
 zV[r,rt,Role,CourseSession] <= 1
 zV[c,rt,Role,CourseSession] <= 1
 zV[r,lb,Role,CourseSession] <= 1
 zV[c,lb,Role,CourseSession] <= 1
 zV[r,rt,Role,Clinic] <= 1
 zV[c,rt,Role,Clinic] <= 1
 zV[r,lb,Role,Clinic] <= 1
 zV[c,lb,Role,Clinic] <= 1
 zV[r,rt,Role,Location] <= 1
 zV[c,rt,Role,Location] <= 1
 zV[r,lb,Role,Location] <= 1
 zV[c,lb,Role,Location] <= 1
 zV[r,rt,Role,Therapist] <= 1
 zV[c,rt,Role,Therapist] <= 1
 zV[r,lb,Role,Therapist] <= 1
 zV[c,lb,Role,Therapist] <= 1
 zV[r,rt,Role,Instructor] <= 1
 zV[c,rt,Role,Instructor] <= 1
 zV[r,lb,Role,Instructor] <= 1
 zV[c,lb,Role,Instructor] <= 1
 zV[r,rt,User,CourseSession] <= 1
 zV[c,rt,User,CourseSession] <= 1
 zV[r,lb,User,CourseSession] <= 1
 zV[c,lb,User,CourseSession] <= 1
 zV[r,rt,User,Clinic] <= 1
 zV[c,rt,User,Clinic] <= 1
 zV[r,lb,User,Clinic] <= 1
 zV[c,lb,User,Clinic] <= 1
 zV[r,rt,User,Location] <= 1
 zV[c,rt,User,Location] <= 1
 zV[r,lb,User,Location] <= 1
 zV[c,lb,User,Location] <= 1
 zV[r,rt,User,Therapist] <= 1
 zV[c,rt,User,Therapist] <= 1
 zV[r,lb,User,Therapist] <= 1
 zV[c,lb,User,Therapist] <= 1
 zV[r,rt,User,Instructor] <= 1
 zV[c,rt,User,Instructor] <= 1
 zV[r,lb,User,Instructor] <= 1
 zV[c,lb,User,Instructor] <= 1
 zV[r,rt,CourseSession,Clinic] <= 1
 zV[c,rt,CourseSession,Clinic] <= 1
 zV[r,lb,CourseSession,Clinic] <= 1
 zV[c,lb,CourseSession,Clinic] <= 1
 zV[r,rt,CourseSession,Location] <= 1
 zV[c,rt,CourseSession,Location] <= 1
 zV[r,lb,CourseSession,Location] <= 1
 zV[c,lb,CourseSession,Location] <= 1
 zV[r,rt,CourseSession,Therapist] <= 1
 zV[c,rt,CourseSession,Therapist] <= 1
 zV[r,lb,CourseSession,Therapist] <= 1
 zV[c,lb,CourseSession,Therapist] <= 1
 zV[r,rt,CourseSession,Instructor] <= 1
 zV[c,rt,CourseSession,Instructor] <= 1
 zV[r,lb,CourseSession,Instructor] <= 1
 zV[c,lb,CourseSession,Instructor] <= 1
 zV[r,rt,Clinic,Location] <= 1
 zV[c,rt,Clinic,Location] <= 1
 zV[r,lb,Clinic,Location] <= 1
 zV[c,lb,Clinic,Location] <= 1
 zV[r,rt,Clinic,Therapist] <= 1
 zV[c,rt,Clinic,Therapist] <= 1
 zV[r,lb,Clinic,Therapist] <= 1
 zV[c,lb,Clinic,Therapist] <= 1
 zV[r,rt,Clinic,Instructor] <= 1
 zV[c,rt,Clinic,Instructor] <= 1
 zV[r,lb,Clinic,Instructor] <= 1
 zV[c,lb,Clinic,Instructor] <= 1
 zV[r,rt,Location,Therapist] <= 1
 zV[c,rt,Location,Therapist] <= 1
 zV[r,lb,Location,Therapist] <= 1
 zV[c,lb,Location,Therapist] <= 1
 zV[r,rt,Location,Instructor] <= 1
 zV[c,rt,Location,Instructor] <= 1
 zV[r,lb,Location,Instructor] <= 1
 zV[c,lb,Location,Instructor] <= 1
 zV[r,rt,Therapist,Instructor] <= 1
 zV[c,rt,Therapist,Instructor] <= 1
 zV[r,lb,Therapist,Instructor] <= 1
 zV[c,lb,Therapist,Instructor] <= 1
Generals
 x[r,Client] x[r,CertificateTemplate] x[r,Course] x[r,Role] x[r,User]
 x[r,CourseSession] x[r,Clinic] x[r,Location] x[r,Therapist]
 x[r,Instructor] x[c,Client] x[c,CertificateTemplate] x[c,Course] x[c,Role]
 x[c,User] x[c,CourseSession] x[c,Clinic] x[c,Location] x[c,Therapist]
 x[c,Instructor] zV[r,rt,Client,CertificateTemplate]
 zV[c,rt,Client,CertificateTemplate] zV[r,lb,Client,CertificateTemplate]
 zV[c,lb,Client,CertificateTemplate] zV[r,rt,Client,Course]
 zV[c,rt,Client,Course] zV[r,lb,Client,Course] zV[c,lb,Client,Course]
 zV[r,rt,Client,Role] zV[c,rt,Client,Role] zV[r,lb,Client,Role]
 zV[c,lb,Client,Role] zV[r,rt,Client,User] zV[c,rt,Client,User]
 zV[r,lb,Client,User] zV[c,lb,Client,User] zV[r,rt,Client,CourseSession]
 zV[c,rt,Client,CourseSession] zV[r,lb,Client,CourseSession]
 zV[c,lb,Client,CourseSession] zV[r,rt,Client,Clinic]
 zV[c,rt,Client,Clinic] zV[r,lb,Client,Clinic] zV[c,lb,Client,Clinic]
 zV[r,rt,Client,Location] zV[c,rt,Client,Location] zV[r,lb,Client,Location]
 zV[c,lb,Client,Location] zV[r,rt,Client,Therapist]
 zV[c,rt,Client,Therapist] zV[r,lb,Client,Therapist]
 zV[c,lb,Client,Therapist] zV[r,rt,Client,Instructor]
 zV[c,rt,Client,Instructor] zV[r,lb,Client,Instructor]
 zV[c,lb,Client,Instructor] zV[r,rt,CertificateTemplate,Course]
 zV[c,rt,CertificateTemplate,Course] zV[r,lb,CertificateTemplate,Course]
 zV[c,lb,CertificateTemplate,Course] zV[r,rt,CertificateTemplate,Role]
 zV[c,rt,CertificateTemplate,Role] zV[r,lb,CertificateTemplate,Role]
 zV[c,lb,CertificateTemplate,Role] zV[r,rt,CertificateTemplate,User]
 zV[c,rt,CertificateTemplate,User] zV[r,lb,CertificateTemplate,User]
 zV[c,lb,CertificateTemplate,User]
 zV[r,rt,CertificateTemplate,CourseSession]
 zV[c,rt,CertificateTemplate,CourseSession]
 zV[r,lb,CertificateTemplate,CourseSession]
 zV[c,lb,CertificateTemplate,CourseSession]
 zV[r,rt,CertificateTemplate,Clinic] zV[c,rt,CertificateTemplate,Clinic]
 zV[r,lb,CertificateTemplate,Clinic] zV[c,lb,CertificateTemplate,Clinic]
 zV[r,rt,CertificateTemplate,Location]
 zV[c,rt,CertificateTemplate,Location]
 zV[r,lb,CertificateTemplate,Location]
 zV[c,lb,CertificateTemplate,Location]
 zV[r,rt,CertificateTemplate,Therapist]
 zV[c,rt,CertificateTemplate,Therapist]
 zV[r,lb,CertificateTemplate,Therapist]
 zV[c,lb,CertificateTemplate,Therapist]
 zV[r,rt,CertificateTemplate,Instructor]
 zV[c,rt,CertificateTemplate,Instructor]
 zV[r,lb,CertificateTemplate,Instructor]
 zV[c,lb,CertificateTemplate,Instructor] zV[r,rt,Course,Role]
 zV[c,rt,Course,Role] zV[r,lb,Course,Role] zV[c,lb,Course,Role]
 zV[r,rt,Course,User] zV[c,rt,Course,User] zV[r,lb,Course,User]
 zV[c,lb,Course,User] zV[r,rt,Course,CourseSession]
 zV[c,rt,Course,CourseSession] zV[r,lb,Course,CourseSession]
 zV[c,lb,Course,CourseSession] zV[r,rt,Course,Clinic]
 zV[c,rt,Course,Clinic] zV[r,lb,Course,Clinic] zV[c,lb,Course,Clinic]
 zV[r,rt,Course,Location] zV[c,rt,Course,Location] zV[r,lb,Course,Location]
 zV[c,lb,Course,Location] zV[r,rt,Course,Therapist]
 zV[c,rt,Course,Therapist] zV[r,lb,Course,Therapist]
 zV[c,lb,Course,Therapist] zV[r,rt,Course,Instructor]
 zV[c,rt,Course,Instructor] zV[r,lb,Course,Instructor]
 zV[c,lb,Course,Instructor] zV[r,rt,Role,User] zV[c,rt,Role,User]
 zV[r,lb,Role,User] zV[c,lb,Role,User] zV[r,rt,Role,CourseSession]
 zV[c,rt,Role,CourseSession] zV[r,lb,Role,CourseSession]
 zV[c,lb,Role,CourseSession] zV[r,rt,Role,Clinic] zV[c,rt,Role,Clinic]
 zV[r,lb,Role,Clinic] zV[c,lb,Role,Clinic] zV[r,rt,Role,Location]
 zV[c,rt,Role,Location] zV[r,lb,Role,Location] zV[c,lb,Role,Location]
 zV[r,rt,Role,Therapist] zV[c,rt,Role,Therapist] zV[r,lb,Role,Therapist]
 zV[c,lb,Role,Therapist] zV[r,rt,Role,Instructor] zV[c,rt,Role,Instructor]
 zV[r,lb,Role,Instructor] zV[c,lb,Role,Instructor]
 zV[r,rt,User,CourseSession] zV[c,rt,User,CourseSession]
 zV[r,lb,User,CourseSession] zV[c,lb,User,CourseSession]
 zV[r,rt,User,Clinic] zV[c,rt,User,Clinic] zV[r,lb,User,Clinic]
 zV[c,lb,User,Clinic] zV[r,rt,User,Location] zV[c,rt,User,Location]
 zV[r,lb,User,Location] zV[c,lb,User,Location] zV[r,rt,User,Therapist]
 zV[c,rt,User,Therapist] zV[r,lb,User,Therapist] zV[c,lb,User,Therapist]
 zV[r,rt,User,Instructor] zV[c,rt,User,Instructor] zV[r,lb,User,Instructor]
 zV[c,lb,User,Instructor] zV[r,rt,CourseSession,Clinic]
 zV[c,rt,CourseSession,Clinic] zV[r,lb,CourseSession,Clinic]
 zV[c,lb,CourseSession,Clinic] zV[r,rt,CourseSession,Location]
 zV[c,rt,CourseSession,Location] zV[r,lb,CourseSession,Location]
 zV[c,lb,CourseSession,Location] zV[r,rt,CourseSession,Therapist]
 zV[c,rt,CourseSession,Therapist] zV[r,lb,CourseSession,Therapist]
 zV[c,lb,CourseSession,Therapist] zV[r,rt,CourseSession,Instructor]
 zV[c,rt,CourseSession,Instructor] zV[r,lb,CourseSession,Instructor]
 zV[c,lb,CourseSession,Instructor] zV[r,rt,Clinic,Location]
 zV[c,rt,Clinic,Location] zV[r,lb,Clinic,Location] zV[c,lb,Clinic,Location]
 zV[r,rt,Clinic,Therapist] zV[c,rt,Clinic,Therapist]
 zV[r,lb,Clinic,Therapist] zV[c,lb,Clinic,Therapist]
 zV[r,rt,Clinic,Instructor] zV[c,rt,Clinic,Instructor]
 zV[r,lb,Clinic,Instructor] zV[c,lb,Clinic,Instructor]
 zV[r,rt,Location,Therapist] zV[c,rt,Location,Therapist]
 zV[r,lb,Location,Therapist] zV[c,lb,Location,Therapist]
 zV[r,rt,Location,Instructor] zV[c,rt,Location,Instructor]
 zV[r,lb,Location,Instructor] zV[c,lb,Location,Instructor]
 zV[r,rt,Therapist,Instructor] zV[c,rt,Therapist,Instructor]
 zV[r,lb,Therapist,Instructor] zV[c,lb,Therapist,Instructor]
End
